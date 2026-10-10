"""Vérification complète de l'environnement Lakehouse.

Usage : docker compose exec jupyter python /opt/lakehouse/scripts/check_env.py
"""
import os
import sys
import traceback

import boto3
import psycopg2
import requests

RESULTATS = []


def test(nom):
    def deco(fonction):
        def wrapper(*args):
            try:
                detail = fonction(*args)
                RESULTATS.append(("OK", nom, detail or ""))
            except Exception as err:  # on veut voir TOUTES les erreurs
                RESULTATS.append(("KO", nom, f"{type(err).__name__}: {err}"))
                if os.environ.get("DEBUG"):
                    traceback.print_exc()
        return wrapper
    return deco


@test("1. Stockage S3 (RustFS)")
def test_s3():
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    noms = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    manquants = {"landing", "lakehouse"} - noms
    assert not manquants, f"buckets manquants : {manquants}"
    return f"buckets : {sorted(noms)}"


@test("2. Catalogue Iceberg REST")
def test_catalogue():
    r = requests.get(f"{os.environ['CATALOG_URI']}/v1/config", timeout=10)
    r.raise_for_status()
    return f"HTTP {r.status_code}"


@test("3. PostgreSQL")
def test_postgres():
    with psycopg2.connect(host=os.environ["PG_HOST"], dbname=os.environ["PG_DB"],
                          user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"],
                          connect_timeout=10) as cnx:
        with cnx.cursor() as cur:
            cur.execute("SELECT version()")
            return cur.fetchone()[0].split(",")[0]


def spark_session():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("check_env").getOrCreate()


@test("4. Spark + Iceberg")
def test_iceberg(spark):
    for couche in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{couche}")
    spark.sql("DROP TABLE IF EXISTS lakehouse.bronze.smoke_test PURGE")
    spark.sql("CREATE TABLE lakehouse.bronze.smoke_test (id INT, msg STRING) USING iceberg")
    spark.sql("INSERT INTO lakehouse.bronze.smoke_test VALUES (1, 'bonjour'), (2, 'lakehouse')")
    n = spark.table("lakehouse.bronze.smoke_test").count()
    snaps = spark.sql("SELECT * FROM lakehouse.bronze.smoke_test.snapshots").count()
    assert n == 2, f"2 lignes attendues, {n} obtenues"
    return f"{n} lignes, {snaps} snapshot(s), Spark {spark.version}"


@test("5. Spark + Delta Lake")
def test_delta(spark):
    chemin = "s3a://lakehouse/delta/smoke_test"
    spark.range(5).write.format("delta").mode("overwrite").save(chemin)
    n = spark.read.format("delta").load(chemin).count()
    versions = spark.sql(f"DESCRIBE HISTORY delta.`{chemin}`").count()
    assert n == 5
    return f"{n} lignes, {versions} version(s) dans l'historique"


@test("6. DuckDB lit les fichiers sur S3")
def test_duckdb(spark):
    import duckdb
    spark.range(10).write.mode("overwrite").parquet("s3a://landing/_smoke/parquet")
    con = duckdb.connect()
    con.sql("LOAD httpfs")
    con.sql(f"""CREATE SECRET s3_local (TYPE s3,
        KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}',
        SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}',
        REGION '{os.environ.get('AWS_REGION', 'us-east-1')}',
        ENDPOINT 'rustfs:9000', URL_STYLE 'path', USE_SSL false)""")
    n = con.sql("SELECT count(*) FROM read_parquet('s3://landing/_smoke/parquet/*.parquet')").fetchone()[0]
    assert n == 10
    return f"{n} lignes, DuckDB {duckdb.__version__}"


@test("7. dbt (adaptateurs spark et trino)")
def test_dbt():
    import subprocess
    sortie = subprocess.run(["dbt", "--version"], capture_output=True, text=True, timeout=120)
    texte = sortie.stdout + sortie.stderr
    manquants = [a for a in ("spark", "trino") if a not in texte.lower()]
    assert sortie.returncode == 0 and not manquants, f"adaptateurs absents : {manquants or texte[-200:]}"
    version = next((l.split(":")[-1].strip() for l in texte.splitlines() if "installed" in l.lower()), "?")
    return f"dbt {version}"


def test_trino():
    """Facultatif : uniquement si le profil sql ou full est démarré."""
    import trino
    try:
        cnx = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"), port=8080,
                                  user="etudiant", catalog="lakehouse")
        cur = cnx.cursor()
        cur.execute("SELECT count(*) FROM lakehouse.bronze.smoke_test")
        RESULTATS.append(("OK", "8. Trino lit la table écrite par Spark",
                          f"{cur.fetchone()[0]} lignes"))
    except Exception as err:
        RESULTATS.append(("--", "8. Trino (profil sql/full non démarré ?)", type(err).__name__))


if __name__ == "__main__":
    test_s3()
    test_catalogue()
    test_postgres()
    spark = spark_session()
    test_iceberg(spark)
    test_delta(spark)
    test_duckdb(spark)
    test_dbt()
    test_trino()
    spark.stop()

    print("\n" + "=" * 72)
    for statut, nom, detail in RESULTATS:
        print(f"[{statut}] {nom:<42} {detail}")
    print("=" * 72)
    echecs = [r for r in RESULTATS if r[0] == "KO"]
    print("ENVIRONNEMENT PRÊT" if not echecs else f"{len(echecs)} TEST(S) EN ÉCHEC")
    sys.exit(1 if echecs else 0)

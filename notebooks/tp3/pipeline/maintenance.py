"""Maintenance hebdomadaire des tables Iceberg du TP 3 : compactage et purge de l'historique."""
from datetime import datetime, timedelta

from commun import spark_session

TABLES = ["bronze.photos", "silver.photos", "silver.quarantaine_photos", "gold.photos_vecteurs"]
GARDER = datetime.now() - timedelta(days=7)          # 7 jours de time travel

spark = spark_session("tp3-maintenance")
for t in TABLES:
    if not spark.catalog.tableExists(f"lakehouse.{t}"):
        continue
    avant = spark.sql(f"SELECT COUNT(*) FROM lakehouse.{t}.files").first()[0]
    spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => '{t}')")
    spark.sql(f"CALL lakehouse.system.expire_snapshots(table => '{t}', "
              f"older_than => TIMESTAMP '{GARDER:%Y-%m-%d %H:%M:%S}', retain_last => 1)")
    apres = spark.sql(f"SELECT COUNT(*) FROM lakehouse.{t}.files").first()[0]
    print(f"{t:28s} fichiers : {avant} → {apres}")

"""Test de bout en bout du TP 4 : Kafka, Spark Structured Streaming et Iceberg.

Lancé par le workflow « Tester TP 4 », dans le conteneur JupyterLab, avec le profil streaming démarré.
Chaque étape affiche ses chiffres ; la dernière ligne est « TOUS LES TESTS RÉUSSIS » si tout passe.
"""
import os
import re
import subprocess
import threading
import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BOOT = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = "suivi_colis"
CK = "s3a://lakehouse/checkpoints/tp4"
SCHEMA = ("evenement_id STRING, colis_id STRING, order_id STRING, transporteur STRING, "
          "categorie STRING, code STRING, horodatage STRING, ville STRING, etat STRING")


def etape(titre):
    print(f"\n===== {titre} =====", flush=True)


def verifier(condition, message):
    print(("OK    " if condition else "ÉCHEC ") + message, flush=True)
    if not condition:
        raise SystemExit(1)


def rejouer(*args):
    r = subprocess.run(["python", "/opt/lakehouse/scripts/rejouer_suivi.py", *args],
                       capture_output=True, text=True)
    print(r.stdout.strip().splitlines()[0] if r.stdout else "", "\n", r.stdout.strip().splitlines()[-1] if r.stdout else "",
          r.stderr[-1500:], flush=True)
    r.check_returncode()
    m = re.search(r"Terminé : (\d+) événements .* dont (\d+) messages tronqués", r.stdout)
    return int(m.group(1)), int(m.group(2))


# ---------------------------------------------------------------- 1. Kafka seul
etape("1. Topic et producteur")
admin = AdminClient({"bootstrap.servers": BOOT})
for f in admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)]).values():
    f.result()
partitions = admin.list_topics(timeout=10).topics[TOPIC].partitions
verifier(len(partitions) == 3, f"topic {TOPIC} créé avec {len(partitions)} partitions")

t0 = time.time()
n1, inv1 = rejouer("--depuis", "2018-03-01", "--jusqu-a", "2018-03-15", "--sans-pause")
print(f"rejeu sans pause : {time.time() - t0:.1f} s")

etape("2. Consommateur, groupe et partitions")
c = Consumer({"bootstrap.servers": BOOT, "group.id": "test_tp4", "auto.offset.reset": "earliest",
              "enable.auto.commit": False})
c.subscribe([TOPIC])
partition_de, lus = {}, 0
melange = 0
while lus < n1:
    msg = c.poll(5)
    if msg is None:
        break
    lus += 1
    if msg.key():
        k = msg.key().decode()
        if partition_de.setdefault(k, msg.partition()) != msg.partition():
            melange += 1
c.commit(asynchronous=False)
positions = c.committed([TopicPartition(TOPIC, p) for p in range(3)], timeout=10)
print("offsets validés par partition :", {tp.partition: tp.offset for tp in positions})
c.close()
verifier(lus == n1, f"{lus} messages lus sur {n1} envoyés")
verifier(melange == 0, f"chaque colis reste dans une seule partition ({len(partition_de)} colis)")

# ---------------------------------------------------------------- 3. Bronze en continu
etape("3. Bronze : Kafka vers Iceberg, avec checkpoint")
spark = SparkSession.builder.appName("tester_tp4").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
for ns in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
spark.sql("""CREATE TABLE IF NOT EXISTS bronze.suivi_evenements (
    cle STRING, valeur STRING, kafka_partition INT, kafka_offset BIGINT, recu_le TIMESTAMP) USING iceberg""")


def flux_kafka():
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOT).option("subscribe", TOPIC)
            .option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 20000).load()
            .select(F.col("key").cast("string").alias("cle"), F.col("value").cast("string").alias("valeur"),
                    F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
                    F.col("timestamp").alias("recu_le")))


def bronze(trigger):
    return (flux_kafka().writeStream.format("iceberg").outputMode("append").trigger(**trigger)
            .option("checkpointLocation", f"{CK}/bronze").toTable("bronze.suivi_evenements"))


def compter(table):
    return spark.table(table).count()


t0 = time.time()
q = bronze({"availableNow": True}); q.awaitTermination()
print(f"premier passage : {time.time() - t0:.1f} s, {len(q.recentProgress)} micro-lots")
verifier(compter("bronze.suivi_evenements") == n1, f"bronze contient les {n1} messages")

q = bronze({"availableNow": True}); q.awaitTermination()
verifier(compter("bronze.suivi_evenements") == n1, "relance sans nouveau message : rien n'est relu (checkpoint)")

n2, inv2 = rejouer("--depuis", "2018-03-15", "--jusqu-a", "2018-04-01", "--sans-pause")
q = bronze({"availableNow": True}); q.awaitTermination()
total = compter("bronze.suivi_evenements")
doublons = total - spark.table("bronze.suivi_evenements").select("kafka_partition", "kafka_offset").distinct().count()
verifier(total == n1 + n2 and doublons == 0, f"après le 2e rejeu : {total} = {n1} + {n2}, aucun offset en double")

# ---------------------------------------------------------------- 4. Silver : parsing, quarantaine, doublons
etape("4. Silver : JSON, quarantaine et doublons (watermark)")
spark.sql(f"""CREATE TABLE IF NOT EXISTS silver.suivi_evenements ({SCHEMA}, horodatage_ts TIMESTAMP,
    recu_le TIMESTAMP) USING iceberg""")
spark.sql("CREATE TABLE IF NOT EXISTS silver.suivi_quarantaine (valeur STRING, recu_le TIMESTAMP) USING iceberg")

lu = (spark.readStream.table("bronze.suivi_evenements")
      .withColumn("e", F.from_json("valeur", SCHEMA)))
quarantaine = (lu.filter(F.col("e.evenement_id").isNull()).select("valeur", "recu_le")
               .writeStream.format("iceberg").trigger(availableNow=True)
               .option("checkpointLocation", f"{CK}/quarantaine").toTable("silver.suivi_quarantaine"))
valides = (lu.filter(F.col("e.evenement_id").isNotNull()).select("e.*", "recu_le")
           .withColumn("horodatage_ts", F.to_timestamp("horodatage"))
           .withWatermark("horodatage_ts", "2 days")
           .dropDuplicatesWithinWatermark(["evenement_id"])
           .select(*[c.split()[0] for c in SCHEMA.split(", ")], "horodatage_ts", "recu_le")
           .writeStream.format("iceberg").trigger(availableNow=True)
           .option("checkpointLocation", f"{CK}/silver").toTable("silver.suivi_evenements"))
quarantaine.awaitTermination(); valides.awaitTermination()

n_q = compter("silver.suivi_quarantaine")
n_s = compter("silver.suivi_evenements")
n_ids = spark.table("silver.suivi_evenements").select("evenement_id").distinct().count()
print(f"silver : {n_s} événements, quarantaine : {n_q}, doublons écartés : {total - n_q - n_s}")
verifier(n_q == inv1 + inv2, f"quarantaine = messages tronqués envoyés ({inv1 + inv2})")
verifier(n_s == n_ids, "aucun evenement_id en double dans silver")

# ---------------------------------------------------------------- 5. Fenêtres et état courant
etape("5. Gold : incidents par jour et par transporteur (fenêtres)")
fenetres = (spark.readStream.table("silver.suivi_evenements")
            .filter("categorie = 'incident'")
            .withWatermark("horodatage_ts", "12 hours")
            .groupBy(F.window("horodatage_ts", "1 day"), "transporteur").count()
            .select(F.col("window.start").alias("jour"), "transporteur", F.col("count").alias("incidents"))
            .writeStream.format("iceberg").outputMode("append").trigger(availableNow=True)
            .option("checkpointLocation", f"{CK}/incidents").toTable("gold.incidents_par_jour"))
fenetres.awaitTermination()
spark.table("gold.incidents_par_jour").orderBy("jour", "transporteur").show(8, truncate=False)
verifier(compter("gold.incidents_par_jour") > 0, "des fenêtres closes ont été écrites")

etape("6. Silver : état courant de chaque colis (MERGE dans foreachBatch)")
spark.sql(f"""CREATE TABLE IF NOT EXISTS silver.colis_etat (colis_id STRING, transporteur STRING,
    code STRING, horodatage_ts TIMESTAMP, ville STRING, etat STRING) USING iceberg""")


def mettre_a_jour(lot, id_lot):
    w = Window.partitionBy("colis_id").orderBy(F.col("horodatage_ts").desc())
    (lot.filter("categorie = 'etape'").withColumn("r", F.row_number().over(w)).filter("r = 1")
        .select("colis_id", "transporteur", "code", "horodatage_ts", "ville", "etat")
        .createOrReplaceTempView("maj"))
    lot.sparkSession.sql("""MERGE INTO silver.colis_etat t USING maj s ON t.colis_id = s.colis_id
        WHEN MATCHED AND s.horodatage_ts > t.horodatage_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")


etat = (spark.readStream.table("silver.suivi_evenements")
        .writeStream.foreachBatch(mettre_a_jour).trigger(availableNow=True)
        .option("checkpointLocation", f"{CK}/colis_etat").start())
etat.awaitTermination()
spark.sql("SELECT code, count(*) AS colis FROM silver.colis_etat GROUP BY code ORDER BY colis DESC").show()
n_colis = spark.table("silver.suivi_evenements").filter("categorie = 'etape'").select("colis_id").distinct().count()
verifier(compter("silver.colis_etat") == n_colis, f"une ligne par colis ({n_colis})")

# ---------------------------------------------------------------- 7. Flux continu pendant un rejeu
etape("7. Flux continu : traitement toutes les 5 s pendant un rejeu à 1 jour par seconde")
avant = compter("bronze.suivi_evenements")
q = bronze({"processingTime": "5 seconds"})
resultat = {}
th = threading.Thread(target=lambda: resultat.update(n=rejouer("--depuis", "2018-04-01", "--jusqu-a", "2018-04-15")))
t0 = time.time(); th.start(); th.join()
time.sleep(12)
q.stop()
apres = compter("bronze.suivi_evenements")
lots = [p for p in q.recentProgress if p["numInputRows"] > 0]
print(f"rejeu de 14 jours en {time.time() - t0:.0f} s, {len(lots)} micro-lots non vides")
for p in lots[:6]:
    print(f"  lot {p['batchId']} : {p['numInputRows']} lignes, {p['durationMs'].get('triggerExecution', 0)} ms")
verifier(apres - avant == resultat["n"][0], f"{apres - avant} nouvelles lignes = {resultat['n'][0]} envoyées")
latence = spark.sql("""SELECT round(avg(unix_timestamp(current_timestamp()) - unix_timestamp(recu_le)))
    FROM bronze.suivi_evenements""").first()[0]
print(f"(indicatif) âge moyen des lignes de bronze : {latence} s")

# ---------------------------------------------------------------- 8. Batch et petits fichiers
etape("8. Le même topic lu en batch, puis compactage")
t0 = time.time()
n_batch = (spark.read.format("kafka").option("kafka.bootstrap.servers", BOOT).option("subscribe", TOPIC)
           .option("startingOffsets", "earliest").option("endingOffsets", "latest").load().count())
print(f"lecture batch du topic : {n_batch} messages en {time.time() - t0:.1f} s")
verifier(n_batch == apres, "le batch lit autant de messages que le flux en a écrit")
for t in ("bronze.suivi_evenements", "silver.suivi_evenements"):
    avant = spark.sql(f"SELECT count(*) FROM {t}.files").first()[0]
    spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => '{t}', options => map('min-input-files','2'))")
    apres_c = spark.sql(f"SELECT count(*) FROM {t}.files").first()[0]
    print(f"{t} : {avant} fichiers -> {apres_c}")
    verifier(apres_c <= avant, f"compactage de {t}")

spark.stop()
print("\nTOUS LES TESTS RÉUSSIS")

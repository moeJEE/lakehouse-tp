"""Flux déclenché du TP 4 : traite les nouveaux événements du topic suivi_colis, puis s'arrête.

Lancé chaque heure par le DAG suivi_incidents. Le checkpoint retient les offsets déjà lus et l'état
de l'agrégation : chaque passage ne lit que les messages arrivés depuis le précédent. La table
gold.passages_incidents garde une ligne par passage (heure, messages traités, durée).
"""
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "suivi_colis"
CHECKPOINT = "s3a://lakehouse/checkpoints/tp4/incidents_airflow"
SCHEMA = ("evenement_id STRING, colis_id STRING, order_id STRING, transporteur STRING, "
          "categorie STRING, code STRING, horodatage STRING, ville STRING, etat STRING")

spark = (SparkSession.builder.appName("tp4-incidents-declenche")
         .config("spark.openlineage.appName", "tp4-incidents-declenche").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
spark.sql("""CREATE TABLE IF NOT EXISTS lakehouse.gold.passages_incidents (
    debut TIMESTAMP, messages BIGINT, duree_s DOUBLE) USING iceberg""")

debut, horloge = datetime.now(), time.time()
evenements = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA).option("subscribe", TOPIC)
              .option("startingOffsets", "earliest").load()
              .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*"))
requete = (evenements.filter("categorie = 'incident'")
           .groupBy("transporteur").agg(F.count("*").alias("incidents"))
           .writeStream.outputMode("complete")
           .trigger(availableNow=True)
           .option("checkpointLocation", CHECKPOINT)
           .toTable("lakehouse.gold.incidents_horaire"))
requete.awaitTermination()

messages = sum(p["numInputRows"] for p in requete.recentProgress)
duree = time.time() - horloge
(spark.createDataFrame([(debut, messages, duree)], "debut TIMESTAMP, messages BIGINT, duree_s DOUBLE")
      .writeTo("lakehouse.gold.passages_incidents").append())
print(f"{messages} nouveau(x) message(s) traité(s) en {duree:.1f} s")
spark.stop()

"""Étape Bronze : ingère les nouvelles photos de la zone d'atterrissage, et les fiches produits.

Incrémental et idempotent : une photo déjà ingérée ne l'est pas une seconde fois, et relancer
l'étape sans nouvelle photo ne change rien.
"""
from pyspark.sql import functions as F

from commun import LANDING, memoriser_ingerees, photos_non_ingerees, spark_session

nouvelles = photos_non_ingerees()
print(f"{len(nouvelles)} nouvelle(s) photo(s) à ingérer")

if nouvelles:
    spark = spark_session("tp3-bronze-photos")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    photos = (spark.read.format("binaryFile")
              .load([f"s3a://{LANDING}/{c}" for c in nouvelles])
              .select(F.col("path").alias("chemin"),
                      F.regexp_extract("path", r"vendeur=(\d+)", 1).alias("vendeur"),
                      F.col("length").alias("taille_octets"),
                      F.sha2("content", 256).alias("sha256"),
                      F.col("content").alias("contenu"),
                      F.lit("photo vendeur").alias("_source"),
                      F.current_timestamp().alias("_ingere_le")))
    if spark.catalog.tableExists("lakehouse.bronze.photos"):
        photos.writeTo("lakehouse.bronze.photos").append()
    else:
        photos.writeTo("lakehouse.bronze.photos").using("iceberg").create()

    # Les fiches produits (petit fichier JSON) sont rechargées en entier à chaque exécution
    (spark.read.text(f"s3a://{LANDING}/catalogue/produits.jsonl")
        .withColumn("_source", F.col("_metadata.file_path"))
        .withColumn("_ingere_le", F.current_timestamp())
        .writeTo("lakehouse.bronze.produits_brut").using("iceberg").createOrReplace())

    memoriser_ingerees(nouvelles)
    print("bronze.photos :", spark.table("lakehouse.bronze.photos").count(), "photos au total")

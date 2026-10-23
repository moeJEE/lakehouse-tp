"""Étape Silver : contrôle des nouvelles photos de Bronze.

Chaque photo est décodée. Elle part en quarantaine si elle est illisible, trop petite, ou si le
produit a déjà une photo acceptée (même fichier, ou autre version de la même photo).
"""
import pandas as pd
from pyspark.sql import functions as F

from commun import TAILLE_MIN, analyser, spark_session

spark = spark_session("tp3-silver-photos")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
bronze = spark.table("lakehouse.bronze.photos").dropDuplicates(["chemin"])

deja_vues = None
for table in ["lakehouse.silver.photos", "lakehouse.silver.quarantaine_photos"]:
    if spark.catalog.tableExists(table):
        vues = spark.table(table).select("chemin")
        deja_vues = vues if deja_vues is None else deja_vues.union(vues)
nouvelles = bronze if deja_vues is None else bronze.join(deja_vues, "chemin", "left_anti")


def analyser_lot(lots):
    for lot in lots:
        resultats = [analyser(bytes(o)) for o in lot["contenu"]]
        lot = lot.drop(columns=["contenu"])
        lot[["valide", "format", "largeur", "hauteur"]] = pd.DataFrame(resultats, index=lot.index)
        yield lot


schema = "chemin string, vendeur string, sha256 string, valide boolean, format string, largeur int, hauteur int"
analyse = (nouvelles.select("chemin", "vendeur", "sha256", "contenu")
           .mapInPandas(analyser_lot, schema)
           .withColumn("item_id", F.regexp_extract("chemin", r"/([A-Z0-9]+)(_copie|_v2)?\.jpg$", 1))
           .toPandas())
# À produit égal, la photo d'origine (nom le plus court) passe en premier
analyse = analyse.assign(_ordre=analyse.chemin.str.len()).sort_values(["item_id", "_ordre"]).drop(columns="_ordre")
print(f"{len(analyse)} nouvelle(s) photo(s) à contrôler")

# Photos déjà acceptées lors des exécutions précédentes
if spark.catalog.tableExists("lakehouse.silver.photos"):
    acceptees = spark.table("lakehouse.silver.photos").select("sha256", "item_id").toPandas()
else:
    acceptees = pd.DataFrame(columns=["sha256", "item_id"])
sha_vus, produits_vus = set(acceptees.sha256), set(acceptees.item_id)

raisons = []
for p in analyse.itertuples():
    # À COMPLÉTER : reprenez les règles du notebook 01 (fonction raison_de_rejet) :
    #   "illisible", "trop_petite" (TAILLE_MIN), "doublon_exact" (sha256 dans sha_vus),
    #   "photo_en_double" (item_id dans produits_vus), sinon None ; une photo acceptée ajoute
    #   son sha256 à sha_vus et son item_id à produits_vus.
    raise NotImplementedError("⚠️ À compléter : règles de qualité")
analyse["raison"] = raisons

if len(analyse):
    colonnes = schema + ", item_id string, raison string"
    for c in ["largeur", "hauteur"]:
        analyse[c] = analyse[c].astype("Int64")
    analyse = analyse.astype(object).where(analyse.notna(), None)
    resultat = (spark.createDataFrame(analyse[[c.split()[0] for c in colonnes.split(", ")]], colonnes)
                .withColumn("_controle_le", F.current_timestamp()))
    for table, filtre in [("silver.photos", "raison IS NULL"), ("silver.quarantaine_photos", "raison IS NOT NULL")]:
        lignes = resultat.filter(filtre)
        lignes = lignes.drop("raison") if table == "silver.photos" else lignes
        if spark.catalog.tableExists(f"lakehouse.{table}"):
            lignes.writeTo(f"lakehouse.{table}").append()
        else:
            lignes.writeTo(f"lakehouse.{table}").using("iceberg").create()

print(pd.Series(raisons, dtype=object).fillna("acceptée").value_counts().to_string())

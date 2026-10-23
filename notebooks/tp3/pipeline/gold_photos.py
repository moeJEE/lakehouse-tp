"""Étape Gold : caractéristiques visuelles des photos et contrôle des catégories déclarées.

1. Les nouvelles photos acceptées en Silver reçoivent leur vecteur MobileNet (gold.photos_vecteurs).
2. Un classifieur apprend à reconnaître le type de produit à partir de la photo ; les fiches dont
   le type déclaré par le vendeur ne correspond pas à la photo sont signalées (gold.photos_controle).
"""
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_predict

from commun import spark_session, vecteurs

spark = spark_session("tp3-gold-photos")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

silver = spark.table("lakehouse.silver.photos").select("chemin", "item_id")
if spark.catalog.tableExists("lakehouse.gold.photos_vecteurs"):
    silver = silver.join(spark.table("lakehouse.gold.photos_vecteurs").select("chemin"), "chemin", "left_anti")
a_traiter = (silver.join(spark.table("lakehouse.bronze.photos").select("chemin", "contenu").dropDuplicates(["chemin"]),
                         "chemin").toPandas())
print(f"{len(a_traiter)} photo(s) à vectoriser")

if len(a_traiter):
    v = vecteurs([bytes(o) for o in a_traiter.contenu])
    nouveaux = pd.DataFrame({"chemin": a_traiter.chemin, "item_id": a_traiter.item_id,
                             "vecteur": [list(map(float, x)) for x in v]})
    df = spark.createDataFrame(nouveaux, "chemin string, item_id string, vecteur array<float>")
    if spark.catalog.tableExists("lakehouse.gold.photos_vecteurs"):
        df.writeTo("lakehouse.gold.photos_vecteurs").append()
    else:
        df.writeTo("lakehouse.gold.photos_vecteurs").using("iceberg").create()

# Fiches produits : la couche Silver les tire du JSON brut
produits = (spark.table("lakehouse.bronze.produits_brut")
            .select(F.from_json("value", "item_id string, type_produit string, titre string, vendeur string")
                    .alias("p")).select("p.*"))
donnees = (spark.table("lakehouse.gold.photos_vecteurs").join(produits, "item_id").toPandas())
X, y = np.stack(donnees.vecteur.map(np.array)), donnees.type_produit

# Prédiction « hors échantillon » : chaque photo est classée par un modèle qui ne l'a pas vue
proba = cross_val_predict(LogisticRegression(max_iter=2000, C=0.5), X, y, cv=5, method="predict_proba")
classes = np.array(sorted(y.unique()))
donnees["type_predit"] = classes[proba.argmax(axis=1)]
donnees["confiance"] = proba.max(axis=1).round(3)
donnees["incoherent"] = (donnees.type_predit != donnees.type_produit) & (donnees.confiance >= 0.8)
print(f"exactitude : {(donnees.type_predit == donnees.type_produit).mean():.3f} | "
      f"fiches signalées : {int(donnees.incoherent.sum())}")

controle = donnees[["item_id", "chemin", "titre", "vendeur", "type_produit", "type_predit", "confiance", "incoherent"]]
spark.createDataFrame(controle).writeTo("lakehouse.gold.photos_controle").using("iceberg").createOrReplace()

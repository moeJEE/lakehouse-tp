"""Contrôle de qualité bloquant : si le lot contient trop de photos rejetées, le pipeline s'arrête.

Un échec ici fait échouer la tâche Airflow : les étapes suivantes ne s'exécutent pas, et l'équipe
est prévenue au lieu de publier des données douteuses.
"""
import sys

from commun import spark_session

SEUIL_REJET = 0.10           # au-delà de 10 % de photos rejetées, on bloque

spark = spark_session("tp3-controle")
acceptees = spark.table("lakehouse.silver.photos").count()
rejets = spark.table("lakehouse.silver.quarantaine_photos")
total = acceptees + rejets.count()
taux = rejets.count() / total if total else 0

print(f"photos acceptées : {acceptees} | rejetées : {rejets.count()} ({100 * taux:.1f} %)")
rejets.groupBy("raison").count().show()

# À COMPLÉTER : arrêter le script avec sys.exit("ÉCHEC : ...") si
#   - le nombre de vecteurs dans gold.photos_vecteurs est différent du nombre de photos acceptées ;
#   - le taux de rejet dépasse SEUIL_REJET.
raise NotImplementedError("⚠️ À compléter : contrôles bloquants")
print("contrôle réussi")

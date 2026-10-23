"""Maintenance des tables Iceberg du TP 3, chaque dimanche à 3 h."""
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

TP3 = "/home/etudiant/work/notebooks/mon_travail/tp3"

with DAG(
    dag_id="maintenance_iceberg",
    description="Compactage des fichiers et purge des anciennes versions",
    schedule=None,  # À COMPLÉTER : chaque dimanche à 3 h (expression cron)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tp3", "maintenance"],
) as dag:
    # À COMPLÉTER : une tâche BashOperator « compacter_et_purger » qui lance pipeline/maintenance.py
    pass

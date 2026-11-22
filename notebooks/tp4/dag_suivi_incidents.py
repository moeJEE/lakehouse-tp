"""DAG du TP 4 : chaque heure, traiter les nouveaux événements du suivi des colis (flux déclenché)."""
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

TP4 = "/home/etudiant/work/notebooks/mon_travail/tp4"

with DAG(
    dag_id="suivi_incidents",
    description="Flux déclenché : incidents par transporteur, mis à jour chaque heure",
    schedule=None,  # À COMPLÉTER : toutes les heures
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tp4", "streaming"],
) as dag:
    # À COMPLÉTER : une tâche BashOperator « incidents_declenche » qui lance pipeline/incidents_declenche.py
    # avec le Python du cours : f"cd {TP4}/pipeline && /usr/local/bin/python incidents_declenche.py"
    pass

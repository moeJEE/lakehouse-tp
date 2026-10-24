"""DAG de démonstration (partie 5, fourni) : une tâche qui échoue volontairement.

Il sert à produire une métrique d'échec, pour voir l'alerte Grafana se déclencher.
Il n'est jamais planifié : seul le notebook 05 le lance.
"""
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="panne_simulee",
    description="Échec volontaire, pour tester l'alerte de supervision (TP 3, partie 5)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 0},
    tags=["tp3", "demo"],
) as dag:
    BashOperator(task_id="tache_en_panne",
                 bash_command='echo "Panne simulée : le service distant ne répond pas" && exit 1')

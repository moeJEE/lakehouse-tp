"""DAG du TP 3 : de la photo déposée par un vendeur à la couche Gold, chaque jour.

attendre_photos → bronze_photos → silver_photos → gold_photos → controle_qualite → dbt_build
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

TP3 = "/home/etudiant/work/notebooks/mon_travail/tp3"
DBT = "/home/etudiant/work/notebooks/mon_travail/tp2/dbt_etoile"


def photos_en_attente():
    """Vrai s'il existe dans la zone d'atterrissage des photos que Bronze n'a pas encore ingérées."""
    import sys
    sys.path.insert(0, f"{TP3}/pipeline")
    from commun import photos_non_ingerees
    return len(photos_non_ingerees()) > 0


with DAG(
    dag_id="catalogue_photos",
    description="Ingestion, contrôle et enrichissement des photos du catalogue",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["tp3", "lakehouse"],
) as dag:

    # À COMPLÉTER :
    #   1. attendre = PythonSensor(task_id="attendre_photos", python_callable=photos_en_attente,
    #                              poke_interval=60, timeout=60 * 30, mode="reschedule", soft_fail=True)
    #   2. une tâche BashOperator par script du pipeline (bronze_photos, silver_photos, gold_photos,
    #      controle_qualite) : bash_command=f"cd {TP3}/pipeline && python <script>.py"
    #   3. une tâche BashOperator « dbt_build » : f"cd {DBT}/.. && dbt build --project-dir dbt_etoile"
    #   4. l'ordre : attendre >> bronze >> silver >> gold >> controle >> dbt
    pass

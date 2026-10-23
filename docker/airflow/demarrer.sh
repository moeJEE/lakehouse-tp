#!/usr/bin/env bash
# Démarre Airflow en mode « standalone » (serveur, planificateur, exécuteur local), avec sa base
# de métadonnées dans le PostgreSQL du cours (base « airflow », créée si besoin).
set -e
python - <<'PY'
import os, psycopg2
cnx = psycopg2.connect(host=os.environ["PG_HOST"], dbname=os.environ["PG_DB"],
                       user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"])
cnx.autocommit = True
cur = cnx.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
if not cur.fetchone():
    cur.execute("CREATE DATABASE airflow")
    print("base airflow créée")
PY
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:5432/airflow"
exec airflow standalone

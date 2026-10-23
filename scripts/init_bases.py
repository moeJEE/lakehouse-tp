"""Crée dans le PostgreSQL du cours les bases des outils du TP 3 (Airflow, Marquez), si besoin."""
import os
import sys

import psycopg2

cnx = psycopg2.connect(host=os.environ["PG_HOST"], dbname=os.environ["PG_DB"],
                       user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"])
cnx.autocommit = True
cur = cnx.cursor()
for base in sys.argv[1:]:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (base,))
    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{base}"')
        print("base créée :", base)

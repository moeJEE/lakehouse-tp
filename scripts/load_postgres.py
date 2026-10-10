"""Charge la base transactionnelle « source » du TP 1 dans PostgreSQL.

La base simule le système de commandes d'Olist, vu à deux dates :

  --lot 1 : état de la base au 1er juillet 2018. Les commandes passées avant cette
            date sont chargées telles qu'elles étaient ce jour-là : une commande
            livrée en août apparaît « shipped », sans date de livraison.
  --lot 2 : état final (octobre 2018). Nouvelles commandes et mises à jour de statut.

Chaque ligne de `ventes.orders` porte une colonne `updated_at` : elle permet une
ingestion incrémentale (on ne relit que ce qui a changé depuis la dernière fois).

Usage : docker compose exec jupyter python /opt/lakehouse/scripts/load_postgres.py --lot 1
"""
import argparse
import os
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DATA = os.environ.get("OLIST_DIR", "/home/etudiant/work/data/olist")
COUPURE = pd.Timestamp("2018-07-01 00:00:00")
DATE_LOT = {1: "2018-07-01 00:00:00", 2: "2018-10-18 00:00:00"}
ETAPES = ["created", "approved", "invoiced", "processing", "shipped", "delivered"]

DDL = """
CREATE SCHEMA IF NOT EXISTS ventes;
CREATE TABLE IF NOT EXISTS ventes.orders (
    order_id                      TEXT PRIMARY KEY,
    customer_id                   TEXT NOT NULL,
    order_status                  TEXT NOT NULL,
    order_purchase_timestamp      TIMESTAMP NOT NULL,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    updated_at                    TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS ventes.order_items (
    order_id            TEXT NOT NULL,
    order_item_id       INTEGER NOT NULL,
    product_id          TEXT NOT NULL,
    seller_id           TEXT NOT NULL,
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10, 2),
    freight_value       NUMERIC(10, 2),
    PRIMARY KEY (order_id, order_item_id)
);
CREATE TABLE IF NOT EXISTS ventes.order_payments (
    order_id             TEXT NOT NULL,
    payment_sequential   INTEGER NOT NULL,
    payment_type         TEXT,
    payment_installments INTEGER,
    payment_value        NUMERIC(10, 2),
    PRIMARY KEY (order_id, payment_sequential)
);
"""

COLS_ORDERS = ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
               "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"]
DATES = COLS_ORDERS[3:]


def lire(nom):
    chemin = os.path.join(DATA, f"olist_{nom}_dataset.csv")
    if not os.path.exists(chemin):
        sys.exit(f"[KO] Fichier introuvable : {chemin}\n"
                 f"     Les CSV Olist doivent être dans le dossier data/olist/ du dépôt.")
    return pd.read_csv(chemin, dtype=str)


def etat_au(orders, date):
    """Reconstitue l'état des commandes tel qu'il était à `date`."""
    o = orders[orders.order_purchase_timestamp < date].copy()
    en_cours = o.order_status.isin(ETAPES)
    regles = [  # (colonne de date, statut si l'événement n'a pas encore eu lieu)
        ("order_delivered_customer_date", "shipped"),
        ("order_delivered_carrier_date", "processing"),
        ("order_approved_at", "created"),
    ]
    for col, statut in regles:
        futur = en_cours & o[col].notna() & (o[col] >= date)
        o.loc[futur, col] = pd.NaT
        o.loc[futur, "order_status"] = statut
    return o


def vers_lignes(df, colonnes):
    df = df[colonnes].astype(object).where(df[colonnes].notna(), None)
    return [tuple(r) for r in df.itertuples(index=False, name=None)]


def upsert(cur, table, colonnes, cle, lignes):
    maj = ", ".join(f"{c} = EXCLUDED.{c}" for c in colonnes if c not in cle)
    sql = (f"INSERT INTO ventes.{table} ({', '.join(colonnes)}) VALUES %s "
           f"ON CONFLICT ({', '.join(cle)}) DO UPDATE SET {maj}")
    execute_values(cur, sql, lignes, page_size=5000)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--lot", type=int, choices=[1, 2], required=True)
    lot = parser.parse_args().lot

    orders = lire("orders")
    for col in DATES:
        orders[col] = pd.to_datetime(orders[col])
    items = lire("order_items")
    payments = lire("order_payments")

    if lot == 1:
        cible = etat_au(orders, COUPURE)
        cible["updated_at"] = pd.Timestamp(DATE_LOT[1])
    else:
        avant = etat_au(orders, COUPURE).set_index("order_id")
        cible = orders.copy()
        idx = cible.set_index("order_id")
        commun = idx.index.intersection(avant.index)
        a, b = idx.loc[commun, COLS_ORDERS[1:]], avant.loc[commun, COLS_ORDERS[1:]]
        modifiees = set(commun[~((a == b) | (a.isna() & b.isna())).all(axis=1)])
        nouvelles = set(idx.index.difference(avant.index))
        cible = cible[cible.order_id.isin(modifiees | nouvelles)].copy()
        cible["updated_at"] = pd.Timestamp(DATE_LOT[2])

    ids = set(cible.order_id)
    items = items[items.order_id.isin(ids)]
    payments = payments[payments.order_id.isin(ids)]

    cnx = psycopg2.connect(host=os.environ.get("PG_HOST", "postgres"),
                           dbname=os.environ["PG_DB"], user=os.environ["PG_USER"],
                           password=os.environ["PG_PASSWORD"])
    with cnx, cnx.cursor() as cur:
        cur.execute(DDL)
        if lot == 1:
            cur.execute("TRUNCATE ventes.orders, ventes.order_items, ventes.order_payments")
        upsert(cur, "orders", COLS_ORDERS + ["updated_at"], ["order_id"],
               vers_lignes(cible, COLS_ORDERS + ["updated_at"]))
        upsert(cur, "order_items", list(items.columns), ["order_id", "order_item_id"],
               vers_lignes(items, list(items.columns)))
        upsert(cur, "order_payments", list(payments.columns),
               ["order_id", "payment_sequential"], vers_lignes(payments, list(payments.columns)))
        cur.execute("SELECT count(*) FROM ventes.orders")
        total = cur.fetchone()[0]
    cnx.close()

    print(f"[OK] Lot {lot} chargé : {len(cible)} commandes écrites "
          f"({len(items)} articles, {len(payments)} paiements).")
    print(f"     La table ventes.orders contient maintenant {total} commandes.")


if __name__ == "__main__":
    main()

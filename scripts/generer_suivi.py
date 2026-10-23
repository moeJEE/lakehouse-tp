"""Génère le flux JSON de suivi des colis (TP 2) et le dépose dans s3://landing/olist/suivi/.

Le transporteur envoie un message JSON par colis : ses étapes (prise en charge, transit,
livraison) et, depuis janvier 2018, ses incidents. Les messages sont reconstruits à partir des
vraies dates des commandes Olist ; seuls les lieux de transit, les transporteurs et les incidents
sont simulés, ainsi que deux défauts réels de ce type de flux : des messages renvoyés deux fois et
des messages tronqués. Le résultat est identique à chaque exécution.

Usage (dans le conteneur JupyterLab) :
    python /opt/lakehouse/scripts/generer_suivi.py              # dépôt dans S3
    python /opt/lakehouse/scripts/generer_suivi.py --local DIR  # écriture locale (test)
"""
import argparse
import json
import os
import random
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

import pandas as pd

DATA = Path(os.environ.get("DATA_DIR", "/home/etudiant/work/data/olist"))
DEBUT_V2 = pd.Timestamp("2018-01-01")   # nouvelle version du format des messages

TRANSPORTEURS = [("Correios", 0.55), ("Jadlog", 0.2), ("Total Express", 0.15), ("Loggi", 0.1)]
SERVICES = {"Correios": ["PAC", "SEDEX"], "Jadlog": [".Package", ".Com"],
            "Total Express": ["Standard"], "Loggi": ["Express"]}
CENTRES = {"SP": "Cajamar", "RJ": "Duque De Caxias", "MG": "Contagem", "PR": "Curitiba",
           "RS": "Porto Alegre", "SC": "Itajai", "BA": "Salvador", "PE": "Recife",
           "GO": "Goiania", "DF": "Brasilia", "CE": "Fortaleza", "ES": "Serra"}
FIABILITE = {"Correios": 1.0, "Jadlog": 1.3, "Total Express": 1.7, "Loggi": 0.5}  # risque d'incident relatif
INCIDENTS = ["DESTINATAIRE_ABSENT", "ADRESSE_INCOMPLETE", "COLIS_ENDOMMAGE", "RETARD_CENTRE_TRI"]


def fmt(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def lieu(ville, etat):
    return {"ville": str(ville).title(), "etat": etat}


def construire_messages():
    orders = pd.read_csv(DATA / "olist_orders_dataset.csv",
                         parse_dates=["order_purchase_timestamp", "order_delivered_carrier_date",
                                      "order_delivered_customer_date", "order_estimated_delivery_date"])
    clients = pd.read_csv(DATA / "olist_customers_dataset.csv", dtype=str).set_index("customer_id")
    clients = clients[["customer_city", "customer_state"]].to_dict("index")
    vendeurs = pd.read_csv(DATA / "olist_sellers_dataset.csv", dtype=str).set_index("seller_id")
    vendeurs = vendeurs[["seller_city", "seller_state"]].to_dict("index")
    articles = pd.read_csv(DATA / "olist_order_items_dataset.csv", dtype=str)
    vendeur_commande = articles.drop_duplicates("order_id").set_index("order_id")["seller_id"].to_dict()

    orders = orders[orders.order_delivered_carrier_date.notna()].sort_values("order_id")
    rng = random.Random(42)
    noms, poids = zip(*TRANSPORTEURS)
    messages = []

    for o in orders.itertuples(index=False):
        if o.order_id not in vendeur_commande or o.customer_id not in clients:
            continue
        v = vendeurs[vendeur_commande[o.order_id]]
        c = clients[o.customer_id]
        depart = o.order_delivered_carrier_date
        arrivee = o.order_delivered_customer_date
        transporteur = rng.choices(noms, poids)[0]

        evts = [{"code": "PRIS_EN_CHARGE", "horodatage": fmt(depart),
                 "lieu": lieu(v["seller_city"], v["seller_state"])}]
        fin = arrivee if pd.notna(arrivee) else o.order_estimated_delivery_date
        duree = max(fin - depart, timedelta(hours=6))
        for i, etat in enumerate(dict.fromkeys([v["seller_state"], c["customer_state"]])):
            t = depart + duree * (0.2 + 0.3 * i) + timedelta(minutes=rng.randint(0, 120))
            evts.append({"code": "EN_TRANSIT", "horodatage": fmt(t),
                         "lieu": lieu(CENTRES.get(etat, "Sao Paulo"), etat if etat in CENTRES else "SP")})
        if pd.notna(arrivee):
            evts.append({"code": "EN_LIVRAISON", "horodatage": fmt(arrivee - timedelta(hours=rng.randint(2, 9))),
                         "lieu": lieu(c["customer_city"], c["customer_state"])})
            evts.append({"code": "LIVRE", "horodatage": fmt(arrivee),
                         "lieu": lieu(c["customer_city"], c["customer_state"])})

        dernier = max(e["horodatage"] for e in evts)
        msg = {"colis_id": "CL" + o.order_id[:12].upper(), "order_id": o.order_id,
               "emis_le": dernier, "transporteur": {"nom": transporteur}, "evenements": evts}

        if depart >= DEBUT_V2:                      # format version 2
            msg["version_schema"] = 2
            msg["transporteur"]["service"] = rng.choice(SERVICES[transporteur])
            en_retard = pd.notna(arrivee) and arrivee > o.order_estimated_delivery_date
            incidents = []
            if rng.random() < (0.5 if en_retard else 0.035) * FIABILITE[transporteur]:
                t = depart + duree * rng.uniform(0.3, 0.9)
                incidents.append({"type": rng.choice(INCIDENTS), "horodatage": fmt(t)})
            msg["incidents"] = incidents
        messages.append(msg)

        if rng.random() < 0.006:                    # message renvoyé deux fois par le transporteur
            messages.append(dict(msg))
    return messages


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", help="dossier de sortie local (sinon : S3)")
    args = parser.parse_args()

    par_mois = defaultdict(list)
    for m in construire_messages():
        ligne = json.dumps(m, ensure_ascii=False)
        if int(m["colis_id"][2:6], 16) % 500 == 0:   # environ 0,2 % de messages tronqués
            ligne = ligne[: len(ligne) // 2]
        par_mois[m["emis_le"][:7]].append(ligne)

    if args.local:
        for mois, lignes in par_mois.items():
            d = Path(args.local) / f"mois={mois}"
            d.mkdir(parents=True, exist_ok=True)
            (d / f"suivi-{mois}.json").write_text("\n".join(lignes) + "\n", encoding="utf-8")
    else:
        import boto3
        s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
        for mois, lignes in par_mois.items():
            s3.put_object(Bucket="landing", Key=f"olist/suivi/mois={mois}/suivi-{mois}.json",
                          Body=("\n".join(lignes) + "\n").encode("utf-8"))

    total = sum(len(v) for v in par_mois.values())
    print(f"{total} messages déposés dans {len(par_mois)} fichiers (un par mois).")


if __name__ == "__main__":
    main()

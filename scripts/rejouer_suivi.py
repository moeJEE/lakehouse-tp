"""Rejoue le suivi des colis du TP 2 comme un flux d'événements, dans Kafka ou dans un dossier (TP 4).

Au TP 2, le transporteur envoyait un message par colis, avec toutes ses étapes, une fois le colis livré.
Ici, chaque message est découpé en événements, un par étape (PRIS_EN_CHARGE, EN_TRANSIT, EN_LIVRAISON,
LIVRE) ou par incident, et chaque événement est envoyé au moment où il se produit, en temps accéléré.

Les défauts réels d'un flux sont conservés ou ajoutés :
  - un message renvoyé deux fois par le transporteur donne des événements en double (même evenement_id) ;
  - un message tronqué est envoyé tel quel : c'est un JSON invalide ;
  - une part des événements (--retard, 5 % par défaut) arrive avec 2 à 48 heures de retard.
Le tirage est fixé (--graine) : deux rejeux identiques envoient les mêmes événements dans le même ordre.

Usage, dans un terminal JupyterLab :
    python /opt/lakehouse/scripts/rejouer_suivi.py                         # Kafka, 1 jour Olist par seconde
    python /opt/lakehouse/scripts/rejouer_suivi.py --jours-par-seconde 5   # plus vite
    python /opt/lakehouse/scripts/rejouer_suivi.py --sans-pause            # tout d'un coup
    python /opt/lakehouse/scripts/rejouer_suivi.py --dossier s3://landing/flux/suivi   # un fichier par jour
"""
import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

FORMAT = "%Y-%m-%d %H:%M:%S"
SOURCE = Path(os.environ.get("DATA_DIR", "/home/etudiant/work/data/olist")) / "suivi"


def lire_evenements(source, part_retard, graine):
    """Découpe chaque message du transporteur en événements datés.

    Renvoie une liste de (instant d'envoi, clé, valeur, valide) ; la valeur est la ligne JSON envoyée.
    """
    rng = random.Random(graine)
    sortie = []
    for fichier in sorted(Path(source).glob("mois=*/*.json")):
        for ligne in fichier.read_text(encoding="utf-8").splitlines():
            if not ligne.strip():
                continue
            try:
                m = json.loads(ligne)
            except json.JSONDecodeError:
                # Message tronqué : on l'envoie tel quel, au moment de son émission si on la retrouve.
                emis = re.search(r'"emis_le": "([0-9: -]{19})"', ligne)
                colis = re.search(r'"colis_id": "(\w+)"', ligne)
                if emis:
                    sortie.append((datetime.strptime(emis.group(1), FORMAT),
                                   colis.group(1) if colis else None, ligne, False))
                continue

            base = {"colis_id": m["colis_id"], "order_id": m["order_id"],
                    "transporteur": m["transporteur"]["nom"]}
            evts = [{"evenement_id": f'{m["colis_id"]}-E{i}', **base, "categorie": "etape",
                     "code": e["code"], "horodatage": e["horodatage"],
                     "ville": e["lieu"]["ville"], "etat": e["lieu"]["etat"]}
                    for i, e in enumerate(m["evenements"])]
            evts += [{"evenement_id": f'{m["colis_id"]}-I{j}', **base, "categorie": "incident",
                      "code": inc["type"], "horodatage": inc["horodatage"], "ville": None, "etat": None}
                     for j, inc in enumerate(m.get("incidents", []))]

            for e in evts:
                envoi = datetime.strptime(e["horodatage"], FORMAT)
                if rng.random() < part_retard:            # l'événement arrive en retard
                    envoi += timedelta(hours=rng.uniform(2, 48))
                sortie.append((envoi, e["colis_id"], json.dumps(e, ensure_ascii=False), True))

    sortie.sort(key=lambda x: x[0])
    return sortie


class SortieKafka:
    def __init__(self, bootstrap, topic):
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient
        topics = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10).topics
        if topic not in topics:
            sys.exit(f"Le topic « {topic} » n'existe pas sur {bootstrap}. "
                     "Créez-le d'abord (notebook 02_kafka, étape 1).")
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 50,
                                  "acks": "all", "enable.idempotence": True})

    def envoyer(self, cle, valeur):
        while True:
            try:
                self.producer.produce(self.topic, key=cle, value=valeur.encode("utf-8"))
                break
            except BufferError:                       # file d'attente locale pleine : on vide
                self.producer.poll(0.5)
        self.producer.poll(0)

    def fin_de_jour(self, jour):
        pass

    def fermer(self):
        restants = self.producer.flush(30)
        if restants:
            sys.exit(f"{restants} message(s) non confirmé(s) par Kafka.")


class SortieDossier:
    """Écrit un fichier JSON (un événement par ligne) par jour simulé."""

    def __init__(self, dossier):
        self.dossier = dossier.rstrip("/")
        self.lignes = []
        if self.dossier.startswith("s3://"):
            import boto3
            self.s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
        else:
            self.s3 = None
            Path(self.dossier).mkdir(parents=True, exist_ok=True)

    def envoyer(self, cle, valeur):
        self.lignes.append(valeur)

    def fin_de_jour(self, jour):
        if not self.lignes:
            return
        corps = ("\n".join(self.lignes) + "\n").encode("utf-8")
        nom = f"suivi-{jour}.json"
        if self.s3:
            bucket, _, prefixe = self.dossier[5:].partition("/")
            self.s3.put_object(Bucket=bucket, Key=f"{prefixe}/{nom}", Body=corps)
        else:
            # Écriture sous un nom caché puis renommage : Spark ne lit jamais un fichier à moitié écrit.
            tmp = Path(self.dossier) / f".{nom}.tmp"
            tmp.write_bytes(corps)
            tmp.rename(Path(self.dossier) / nom)
        self.lignes = []

    def fermer(self):
        pass


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--depuis", default="2018-01-01", help="premier jour rejoué (défaut : 2018-01-01)")
    p.add_argument("--jusqu-a", dest="jusqua", default="2018-09-01", help="jour de fin, exclu (défaut : 2018-09-01)")
    p.add_argument("--jours-par-seconde", type=float, default=1.0, help="vitesse du rejeu (défaut : 1)")
    p.add_argument("--sans-pause", action="store_true", help="tout envoyer d'un coup")
    p.add_argument("--retard", type=float, default=0.05, help="part des événements en retard (défaut : 0.05)")
    p.add_argument("--graine", type=int, default=42)
    p.add_argument("--topic", default="suivi_colis")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--dossier", help="écrire dans un dossier (local ou s3://...) au lieu de Kafka")
    p.add_argument("--source", default=str(SOURCE))
    args = p.parse_args()

    debut = datetime.fromisoformat(args.depuis)
    fin = datetime.fromisoformat(args.jusqua)
    evenements = [e for e in lire_evenements(args.source, args.retard, args.graine) if debut <= e[0] < fin]
    if not evenements:
        sys.exit("Aucun événement sur cette période.")

    sortie = SortieDossier(args.dossier) if args.dossier else SortieKafka(args.bootstrap, args.topic)
    destination = args.dossier or f"le topic {args.topic}"
    print(f"{len(evenements)} événements du {debut:%Y-%m-%d} au {fin:%Y-%m-%d} vers {destination}"
          + ("" if args.sans_pause else f", {args.jours_par_seconde:g} jour(s) par seconde") + ".", flush=True)

    depart_reel = time.monotonic()
    jour_courant, n_jour, n_total, n_invalides = None, 0, 0, 0
    for instant, cle, valeur, valide in evenements:
        jour = instant.date().isoformat()
        if jour != jour_courant:
            if jour_courant:
                sortie.fin_de_jour(jour_courant)
                if datetime.fromisoformat(jour_courant).weekday() == 6:   # bilan chaque dimanche
                    print(f"  semaine jusqu'au {jour_courant} : {n_total} événements envoyés", flush=True)
            jour_courant = jour
        if not args.sans_pause:
            cible = (instant - debut).total_seconds() / 86400 / args.jours_par_seconde
            attente = cible - (time.monotonic() - depart_reel)
            if attente > 0:
                time.sleep(attente)
        sortie.envoyer(cle, valeur)
        n_total += 1
        n_invalides += not valide
    sortie.fin_de_jour(jour_courant)
    sortie.fermer()

    duree = time.monotonic() - depart_reel
    print(f"Terminé : {n_total} événements envoyés en {duree:.0f} s, dont {n_invalides} messages tronqués.")


if __name__ == "__main__":
    main()

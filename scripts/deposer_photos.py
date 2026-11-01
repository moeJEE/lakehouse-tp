"""Simule l'envoi des photos par les vendeurs : copie un lot du catalogue dans s3://landing/catalogue/.

    python /opt/lakehouse/scripts/deposer_photos.py --lot 1   # vendeurs 01 à 08 + fiches produits
    python /opt/lakehouse/scripts/deposer_photos.py --lot 2   # vendeurs 09 à 12 (arrivent plus tard)
"""
import argparse
import os
from pathlib import Path

import boto3

CATALOGUE = Path(os.environ.get("CATALOGUE_DIR", "/home/etudiant/work/data/catalogue"))
LOTS = {1: range(1, 9), 2: range(9, 13)}

parser = argparse.ArgumentParser()
parser.add_argument("--lot", type=int, choices=LOTS, required=True)
lot = parser.parse_args().lot

s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
if lot == 1:
    # Le lot 1 ouvre le catalogue : on repart d'une zone d'atterrissage vide (photos d'une exécution
    # précédente, lot 2 compris, et liste des photos déjà ingérées), pour que le TP soit rejouable.
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="landing", Prefix="catalogue/"):
        for o in page.get("Contents", []):
            s3.delete_object(Bucket="landing", Key=o["Key"])
    s3.upload_file(str(CATALOGUE / "produits.jsonl"), "landing", "catalogue/produits.jsonl")
n = 0
for v in LOTS[lot]:
    for photo in sorted((CATALOGUE / "photos" / f"vendeur={v:02d}").glob("*")):
        s3.upload_file(str(photo), "landing", f"catalogue/photos/vendeur={v:02d}/{photo.name}")
        n += 1
print(f"lot {lot} : {n} photos déposées dans s3://landing/catalogue/photos/")

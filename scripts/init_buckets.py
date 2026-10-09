"""Crée les buckets S3 du Lakehouse. Idempotent : peut être relancé sans risque."""
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

BUCKETS = ["landing", "lakehouse"]

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
)

for tentative in range(1, 31):  # attend que RustFS soit prêt
    try:
        s3.list_buckets()
        break
    except (EndpointConnectionError, ClientError) as err:
        print(f"[attente] stockage S3 indisponible ({tentative}/30) : {err}")
        time.sleep(2)
else:
    sys.exit("[KO] Le stockage S3 ne répond pas.")

existants = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
for nom in BUCKETS:
    if nom in existants:
        print(f"[OK] bucket '{nom}' déjà présent")
    else:
        s3.create_bucket(Bucket=nom)
        print(f"[OK] bucket '{nom}' créé")

"""Fonctions partagées par les étapes du pipeline photos (TP 3)."""
import io
import os

import numpy as np

LANDING, PREFIXE = "landing", "catalogue/photos/"
ETAT = "catalogue/_ingerees.txt"           # liste des photos déjà ingérées en Bronze
MODELE_ONNX = "/home/etudiant/work/data/modeles/mobilenet_v3_small.onnx"
TAILLE_MIN = 64                            # en pixels : en dessous, la photo est inutilisable


def s3():
    import boto3
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])


def spark_session(nom):
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName(nom).getOrCreate()


def photos_deposees():
    """Clés de toutes les photos présentes dans la zone d'atterrissage."""
    cles, page = [], s3().get_paginator("list_objects_v2")
    for p in page.paginate(Bucket=LANDING, Prefix=PREFIXE):
        cles += [o["Key"] for o in p.get("Contents", [])]
    return cles


def photos_ingerees():
    try:
        corps = s3().get_object(Bucket=LANDING, Key=ETAT)["Body"].read().decode()
        return set(corps.split())
    except s3().exceptions.NoSuchKey:
        return set()


def photos_non_ingerees():
    deja = photos_ingerees()
    return [c for c in photos_deposees() if c not in deja]


def memoriser_ingerees(cles):
    toutes = sorted(photos_ingerees() | set(cles))
    s3().put_object(Bucket=LANDING, Key=ETAT, Body="\n".join(toutes).encode())


def analyser(octets):
    """Décode une image. Renvoie (valide, format, largeur, hauteur)."""
    from PIL import Image
    try:
        img = Image.open(io.BytesIO(octets))
        img.load()                                   # décode toute l'image : détecte les fichiers tronqués
        return True, img.format, img.width, img.height
    except Exception:
        return False, None, None, None


def vecteurs(liste_octets, lot=32):
    """Caractéristiques visuelles (576 nombres par image) calculées par MobileNet (format ONNX)."""
    import onnxruntime as ort
    from PIL import Image
    session = ort.InferenceSession(MODELE_ONNX, providers=["CPUExecutionProvider"])
    moyenne = np.array([0.485, 0.456, 0.406], dtype=np.float32)
    ecart = np.array([0.229, 0.224, 0.225], dtype=np.float32)
    sortie = []
    for i in range(0, len(liste_octets), lot):
        images = []
        for octets in liste_octets[i:i + lot]:
            img = Image.open(io.BytesIO(octets)).convert("RGB").resize((224, 224))
            x = (np.asarray(img, dtype=np.float32) / 255.0 - moyenne) / ecart
            images.append(x.transpose(2, 0, 1))
        sortie.append(session.run(None, {"image": np.stack(images)})[0])
    return np.concatenate(sortie) if sortie else np.zeros((0, 576), dtype=np.float32)

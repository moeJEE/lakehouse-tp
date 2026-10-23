"""Prépare le jeu d'images du TP 3 à partir d'Amazon Berkeley Objects (ABO, CC BY-NC 4.0).

Sortie (dans data/catalogue/) :
    produits.jsonl              une fiche produit JSON par ligne (sous-ensemble des listings ABO)
    photos/vendeur=XX/*.jpg     les photos envoyées par les vendeurs, avec des défauts volontaires
    LISEZMOI.md                 provenance, licence, défauts ajoutés
et models/mobilenet_v3_small.onnx (extracteur de caractéristiques d'images).

Exécuté une fois par le workflow « Préparer les images » ; inutile de le relancer pendant le TP.
"""
import collections
import gzip
import hashlib
import io
import json
import random
import tarfile
import urllib.request
from pathlib import Path

from PIL import Image

BASE = "https://amazon-berkeley-objects.s3.amazonaws.com/archives/"
SORTIE = Path("data/catalogue")
TYPES_PREFERES = ["CHAIR", "SOFA", "TABLE", "LAMP", "RUG", "PILLOW", "SHOES", "HANDBAG", "WATCH",
                  "BOTTLE", "PLANTER", "MIRROR"]
NB_TYPES, PAR_TYPE, NB_VENDEURS = 6, 500, 12
rng = random.Random(42)


def telecharger(nom):
    chemin = Path("/tmp") / nom
    if not chemin.exists():
        print("téléchargement :", nom, flush=True)
        urllib.request.urlretrieve(BASE + nom, chemin)
    return chemin


def valeur_en(champ):
    for v in champ or []:
        if v.get("language_tag", "en_US").startswith("en"):
            return v.get("value")
    return None


def lire_listings():
    produits = {}
    with tarfile.open(telecharger("abo-listings.tar")) as tar:
        for m in tar:
            if not m.name.endswith(".json.gz"):
                continue
            for ligne in gzip.open(tar.extractfile(m), "rt", encoding="utf-8"):
                p = json.loads(ligne)
                titre = valeur_en(p.get("item_name"))
                type_ = (p.get("product_type") or [{}])[0].get("value")
                if titre and type_ and p.get("main_image_id"):
                    produits[p["item_id"]] = {
                        "item_id": p["item_id"], "type_produit": type_, "titre": titre,
                        "marque": valeur_en(p.get("brand")), "couleur": valeur_en(p.get("color")),
                        "materiau": valeur_en(p.get("material")), "image_id": p["main_image_id"]}
    return produits


def main():
    produits = lire_listings()
    compte = collections.Counter(p["type_produit"] for p in produits.values())
    types = [t for t in TYPES_PREFERES if compte[t] >= PAR_TYPE][:NB_TYPES]
    print("types retenus :", {t: compte[t] for t in types})

    choisis = []
    for t in types:
        groupe = sorted((p for p in produits.values() if p["type_produit"] == t), key=lambda p: p["item_id"])
        choisis += rng.sample(groupe, PAR_TYPE)
    par_image = {p["image_id"]: p for p in choisis}

    # Chemin de chaque image dans l'archive des petites images
    chemins = {}
    with tarfile.open(telecharger("abo-images-small.tar")) as tar:
        for m in tar:
            if m.name.endswith("images.csv.gz"):
                for ligne in gzip.open(tar.extractfile(m), "rt"):
                    image_id, h, w, chemin = ligne.strip().split(",")
                    if image_id in par_image:
                        chemins["images/small/" + chemin] = image_id
                break
    photos = SORTIE / "photos"
    contenu = {}
    with tarfile.open(telecharger("abo-images-small.tar")) as tar:
        for m in tar:
            nom = m.name.split("abo-images-small/")[-1] if "abo-images-small/" in m.name else m.name
            if nom in chemins:
                contenu[chemins[nom]] = tar.extractfile(m).read()
    print(len(contenu), "images extraites sur", len(par_image))

    SORTIE.mkdir(parents=True, exist_ok=True)
    fiches, defauts = [], collections.Counter()
    for i, (image_id, octets) in enumerate(sorted(contenu.items())):
        p = par_image[image_id]
        vendeur = f"vendeur={rng.randint(1, NB_VENDEURS):02d}"
        nom = f"{p['item_id']}.jpg"
        dossier = photos / vendeur
        dossier.mkdir(parents=True, exist_ok=True)
        tirage = rng.random()
        if tirage < 0.006:                                  # fichier coupé pendant l'envoi
            (dossier / nom).write_bytes(octets[: len(octets) // 3]); defauts["tronquee"] += 1
        elif tirage < 0.009:                                # pas une image (mauvais fichier envoyé)
            (dossier / nom).write_text("Fiche produit en attente de photo\n"); defauts["pas_une_image"] += 1
        else:
            (dossier / nom).write_bytes(octets)
        if 0.009 <= tirage < 0.025:                         # même photo envoyée par un autre vendeur
            autre = photos / f"vendeur={rng.randint(1, NB_VENDEURS):02d}"
            autre.mkdir(parents=True, exist_ok=True)
            (autre / f"{p['item_id']}_copie.jpg").write_bytes(octets); defauts["doublon_exact"] += 1
        elif 0.025 <= tirage < 0.035:                       # même photo, recadrée et recompressée
            img = Image.open(io.BytesIO(octets)).convert("RGB")
            w, h = img.size
            img = img.crop((w // 20, h // 20, w - w // 20, h - h // 20)).resize((w * 3 // 4, h * 3 // 4))
            buf = io.BytesIO(); img.save(buf, "JPEG", quality=70)
            (dossier / f"{p['item_id']}_v2.jpg").write_bytes(buf.getvalue()); defauts["quasi_doublon"] += 1
        elif 0.035 <= tirage < 0.045:                       # vignette trop petite
            img = Image.open(io.BytesIO(octets)).convert("RGB")
            img.thumbnail((48, 48)); buf = io.BytesIO(); img.save(buf, "JPEG")
            (dossier / nom).write_bytes(buf.getvalue()); defauts["trop_petite"] += 1
        fiches.append({**{k: v for k, v in p.items() if k != "image_id"}, "vendeur": vendeur.split("=")[1],
                       "photo": f"{vendeur}/{nom}"})

    with open(SORTIE / "produits.jsonl", "w", encoding="utf-8") as f:
        for fiche in fiches:
            f.write(json.dumps(fiche, ensure_ascii=False) + "\n")
    print("fiches :", len(fiches), "| défauts :", dict(defauts))

    (SORTIE / "LISEZMOI.md").write_text(f"""# Catalogue photo (TP 3)

Sous-ensemble d'**Amazon Berkeley Objects (ABO)** : {len(fiches)} produits de {len(types)} types
({', '.join(types)}), {PAR_TYPE} par type, photos de 256 pixels au plus.

- Source : <https://amazon-berkeley-objects.s3.amazonaws.com/index.html>
- Licence : CC BY-NC 4.0. Collins et al., *ABO: Dataset and Benchmarks for Real-World 3D Object
  Understanding*, CVPR 2022. Redistribué ici pour un usage pédagogique.
- `produits.jsonl` : une fiche par produit (titre, type, marque, couleur, matériau, vendeur, photo).
- `photos/vendeur=XX/` : les photos, réparties entre {NB_VENDEURS} vendeurs fictifs.

Défauts ajoutés volontairement pour le TP : {dict(defauts)}.
Produit par `scripts/preparer_images.py`.
""", encoding="utf-8")

    exporter_modele()


def exporter_modele():
    import torch
    import torchvision
    modele = torchvision.models.mobilenet_v3_small(weights="IMAGENET1K_V1").eval()
    extracteur = torch.nn.Sequential(modele.features, modele.avgpool, torch.nn.Flatten())
    Path("models").mkdir(exist_ok=True)
    torch.onnx.export(extracteur, torch.randn(1, 3, 224, 224), "models/mobilenet_v3_small.onnx",
                      input_names=["image"], output_names=["vecteur"],
                      dynamic_axes={"image": {0: "lot"}, "vecteur": {0: "lot"}}, opset_version=17)
    print("modèle exporté : models/mobilenet_v3_small.onnx")


if __name__ == "__main__":
    main()

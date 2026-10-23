"""Traduit en anglais les commentaires des avis Olist (portugais → anglais).

Produit data/olist/olist_order_reviews_dataset_en.csv : mêmes colonnes et mêmes lignes que le fichier
d'origine, avec le titre et le message traduits. Les retours à la ligne sont conservés (chaque ligne
d'un commentaire est traduite séparément), pour que le fichier garde les pièges du fichier réel.

Traduction automatique par un modèle open source (Helsinki-NLP / OPUS-MT). Exécuté une fois par
le workflow « Traduire les avis » ; il n'est pas nécessaire de le relancer pendant le TP.
Dépendances : pip install pandas torch transformers sentencepiece sacremoses
"""
import re
import sys
import time
from pathlib import Path

import pandas as pd
import torch
from transformers import MarianMTModel, MarianTokenizer

DATA = Path(sys.argv[1] if len(sys.argv) > 1 else "data/olist")
SOURCE = DATA / "olist_order_reviews_dataset.csv"
CIBLE = DATA / "olist_order_reviews_dataset_en.csv"
MODELES = ["Helsinki-NLP/opus-mt-ROMANCE-en", "Helsinki-NLP/opus-mt-mul-en"]
COLONNES = ["review_comment_title", "review_comment_message"]
LIGNE = re.compile(r"\r\n|\n|\r")


def charger():
    for nom in MODELES:
        try:
            return nom, MarianTokenizer.from_pretrained(nom), MarianMTModel.from_pretrained(nom).eval()
        except Exception as err:  # modèle introuvable : on essaie le suivant
            print("indisponible :", nom, type(err).__name__)
    raise SystemExit("aucun modèle de traduction disponible")


def main():
    avis = pd.read_csv(SOURCE, dtype=str, keep_default_na=False)
    segments = sorted({s.strip() for c in COLONNES for t in avis[c] for s in LIGNE.split(t) if s.strip()},
                      key=len)
    nom, tok, modele = charger()
    torch.set_num_threads(max(1, torch.get_num_threads()))
    print(f"{len(segments)} segments à traduire avec {nom}")

    traduction, debut, lot = {}, time.time(), 48
    for i in range(0, len(segments), lot):
        paquet = segments[i:i + lot]
        entree = tok(paquet, return_tensors="pt", padding=True, truncation=True, max_length=200)
        with torch.no_grad():
            sortie = modele.generate(**entree, num_beams=2, max_new_tokens=200)
        traduction.update(zip(paquet, tok.batch_decode(sortie, skip_special_tokens=True)))
        if i % (lot * 50) == 0:
            print(f"{i + len(paquet)}/{len(segments)} — {time.time() - debut:.0f} s", flush=True)

    def traduire(texte):
        if not texte:
            return texte
        morceaux = LIGNE.split(texte)
        seps = LIGNE.findall(texte)
        out = [traduction.get(m.strip(), m) if m.strip() else m for m in morceaux]
        return "".join(o + (seps[k] if k < len(seps) else "") for k, o in enumerate(out))

    for c in COLONNES:
        avis[c] = avis[c].map(traduire)
    avis.to_csv(CIBLE, index=False, lineterminator="\n")
    print(f"écrit : {CIBLE} ({len(avis)} avis) en {time.time() - debut:.0f} s")


if __name__ == "__main__":
    main()

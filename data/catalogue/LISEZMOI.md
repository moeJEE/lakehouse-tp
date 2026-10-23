# Catalogue photo (TP 3)

Sous-ensemble d'**Amazon Berkeley Objects (ABO)** : 2906 produits de 6 types
(CHAIR, SOFA, TABLE, RUG, SHOES, HANDBAG), 500 par type, photos de 256 pixels au plus.

- Source : <https://amazon-berkeley-objects.s3.amazonaws.com/index.html>
- Licence : CC BY-NC 4.0. Collins et al., *ABO: Dataset and Benchmarks for Real-World 3D Object
  Understanding*, CVPR 2022. Redistribué ici pour un usage pédagogique.
- `produits.jsonl` : une fiche par produit (titre, type, marque, couleur, matériau, vendeur, photo).
- `photos/vendeur=XX/` : les photos, réparties entre 12 vendeurs fictifs.

Défauts ajoutés volontairement pour le TP : {'tronquee': 17, 'doublon_exact': 46, 'quasi_doublon': 30, 'trop_petite': 30, 'pas_une_image': 10}.
Produit par `scripts/preparer_images.py`.

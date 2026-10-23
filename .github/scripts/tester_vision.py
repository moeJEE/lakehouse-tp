"""Vérifie sur le vrai catalogue les fonctions d'images du TP 3 (sans Spark) et mesure les résultats."""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, "notebooks/tp3/pipeline")
import commun  # noqa: E402

commun.MODELE_ONNX = "data/modeles/mobilenet_v3_small.onnx"
from sklearn.linear_model import LogisticRegression  # noqa: E402
from sklearn.metrics import classification_report  # noqa: E402
from sklearn.model_selection import cross_val_predict  # noqa: E402

CAT = Path("data/catalogue")
fiches = {json.loads(l)["item_id"]: json.loads(l) for l in open(CAT / "produits.jsonl")}
photos = sorted(CAT.glob("photos/vendeur=*/*"))
lignes = [(p, *commun.analyser(p.read_bytes())) for p in photos]
valides = [l for l in lignes if l[1] and min(l[3], l[4]) >= 64]
originaux = [l for l in valides if "_copie" not in l[0].name and "_v2" not in l[0].name]
print("photos :", len(photos), "| valides :", len(valides), "| originaux valides :", len(originaux))

t = time.time()
V = commun.vecteurs([l[0].read_bytes() for l in originaux])
print(f"vecteurs : {V.shape} en {time.time() - t:.0f} s")
Vn = V / np.linalg.norm(V, axis=1, keepdims=True)

# robustesse : photo recadrée (_v2) contre son original
index = {l[0].stem: i for i, l in enumerate(originaux)}
v2 = [l for l in valides if "_v2" in l[0].name and l[0].stem.split("_")[0] in index]
W = commun.vecteurs([l[0].read_bytes() for l in v2])
Wn = W / np.linalg.norm(W, axis=1, keepdims=True)
sim_v2 = [float(Wn[k] @ Vn[index[l[0].stem.split("_")[0]]]) for k, l in enumerate(v2)]
print("similarité original / version recadrée :", np.round(sorted(sim_v2), 3))

S = Vn @ Vn.T
np.fill_diagonal(S, 0)
haut = S[np.triu_indices(len(S), 1)]
for s in [0.9, 0.95, 0.97, 0.98, 0.99]:
    print(f"paires de fiches différentes à similarité >= {s} : {(haut >= s).sum()}")
for a, b in np.argwhere(np.triu(S) >= 0.97)[:8]:
    print(f"  {S[a, b]:.3f} | {fiches[originaux[a][0].stem]['titre'][:55]} | {fiches[originaux[b][0].stem]['titre'][:55]}")

y = np.array([fiches[l[0].stem]["type_produit"] for l in originaux])
pred = cross_val_predict(LogisticRegression(max_iter=2000, C=0.5), V, y, cv=5, method="predict_proba")
classes = np.array(sorted(set(y)))
yp, conf = classes[pred.argmax(1)], pred.max(1)
print(classification_report(y, yp))
inc = (yp != y) & (conf >= 0.8)
print("fiches signalées (confiance >= 0,8) :", inc.sum())
for i in np.where(inc)[0][:15]:
    print("  ", y[i], "->", yp[i], round(conf[i], 2), fiches[originaux[i][0].stem]["titre"][:90])
for i in [0, 700, 1400]:
    voisins = np.argsort(Vn @ Vn[i])[::-1][1:6]
    print("voisins de", y[i], ":", list(y[voisins]))

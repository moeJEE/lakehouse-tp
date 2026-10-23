"""TP 2 — Tableau de bord de la satisfaction client Olist.

Lancement, depuis un terminal JupyterLab (menu File > New > Terminal) :
    streamlit run notebooks/mon_travail/tp2/dashboard_tp2.py --server.address 0.0.0.0 --server.port 8501
puis ouvrir http://localhost:8501 . Le tableau de bord se recharge à chaque enregistrement du fichier.

Documentation utile :
    Streamlit : https://docs.streamlit.io/develop/api-reference
    Plotly Express : https://plotly.com/python/plotly-express/
"""
import re
import unicodedata
from pathlib import Path

import joblib
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="Olist — Satisfaction", page_icon="💬", layout="wide")
MODELE = Path(__file__).parent / "modele_avis.joblib"


@st.cache_resource
def session_spark():
    # 1 Go suffit : on ne lit que des tables Gold déjà agrégées par Spark
    return (SparkSession.builder.appName("dashboard-tp2")
            .config("spark.driver.memory", "1g")
            .getOrCreate())


@st.cache_data(ttl=300)
def requete(sql):
    """Exécute une requête Spark SQL et renvoie un DataFrame pandas."""
    return session_spark().sql(sql).toPandas()


def normaliser(texte):
    """Même normalisation que la partie 2 : minuscules, sans accents ni ponctuation."""
    texte = unicodedata.normalize("NFKD", texte.lower()).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z\s]", " ", texte)).strip()


# ---------- Données (fournies) ----------
mensuel = requete("""
    SELECT d.annee_mois AS mois, COUNT(*) AS nb_avis, AVG(a.note) AS note_moyenne,
           100 * AVG(CAST(a.est_negatif AS INT)) AS pct_negatifs,
           100 * AVG(CAST(a.a_commentaire AS INT)) AS pct_commentes
    FROM lakehouse.gold.fait_avis a JOIN lakehouse.gold.dim_date d ON d.date_id = a.date_avis_id
    GROUP BY d.annee_mois ORDER BY mois""")
retards = requete("""
    SELECT CASE WHEN en_retard THEN 'En retard' ELSE 'À l''heure' END AS livraison,
           COUNT(*) AS nb_avis, AVG(note) AS note_moyenne,
           100 * AVG(CAST(est_negatif AS INT)) AS pct_negatifs
    FROM lakehouse.gold.fait_avis WHERE en_retard IS NOT NULL GROUP BY 1""")
transporteurs = requete("""
    SELECT transporteur, COUNT(*) AS nb_colis,
           100 * AVG(CAST(nb_incidents > 0 AS INT)) AS pct_incidents,
           percentile_approx(duree_transit_h, 0.5) / 24 AS duree_mediane_j
    FROM lakehouse.gold.fait_livraisons WHERE date_expedition_id >= 20180101
    GROUP BY transporteur""")

# ---------- Filtre de période (fourni) ----------
st.sidebar.header("Filtres")
mois = mensuel["mois"].tolist()
debut, fin = st.sidebar.select_slider("Période", options=mois, value=("2017-01", "2018-08")
                                      if {"2017-01", "2018-08"} <= set(mois) else (mois[0], mois[-1]))
periode = mensuel[(mensuel["mois"] >= debut) & (mensuel["mois"] <= fin)]
st.sidebar.caption("Les mois de 2016 sont incomplets : ils sont exclus par défaut.")

st.title("💬 Olist — Satisfaction des clients")
st.caption(f"Période : {debut} → {fin} · Source : schéma en étoile de la couche Gold")

# ---------- 1. Indicateurs clés ----------
# À COMPLÉTER : quatre indicateurs côte à côte (st.columns(4) puis .metric(...)) sur la période :
#   note moyenne, part d'avis négatifs, part d'avis commentés (moyennes pondérées par nb_avis),
#   et part des colis avec incident (tableau transporteurs, pondérée par nb_colis).


# ---------- 2. Évolution mensuelle ----------
# À COMPLÉTER : courbe de la part d'avis négatifs par mois (px.line sur periode)


# ---------- 3. L'effet des retards ----------
# À COMPLÉTER : barres de la part d'avis négatifs, à l'heure / en retard (tableau retards)


# ---------- 4. Transporteurs ----------
# À COMPLÉTER : barres de la part de colis avec incident par transporteur,
#   colorées par la durée médiane de transit (tableau transporteurs)


# ---------- 5. Tester un commentaire (modèle de la partie 4) ----------
# À COMPLÉTER :
#   - si MODELE n'existe pas, afficher un message (st.info) qui invite à exécuter le notebook 04 ;
#   - sinon, charger le modèle (joblib.load), proposer une zone de texte (st.text_area) et afficher
#     la probabilité que le commentaire soit négatif : modele.predict_proba([normaliser(texte)])[0, 1] ;
#   - afficher les avis les plus probablement négatifs de gold.avis_sentiment, au-dessus d'un seuil
#     choisi avec st.slider (fonction requete).

"""TP 1 — Tableau de bord des ventes Olist.

Lancement, depuis un terminal JupyterLab (menu File > New > Terminal) :
    streamlit run notebooks/mon_travail/dashboard_tp1.py --server.address 0.0.0.0 --server.port 8501
puis ouvrir http://localhost:8501 . Le tableau de bord se recharge à chaque enregistrement du fichier.

Documentation utile :
    Streamlit : https://docs.streamlit.io/develop/api-reference
    Plotly Express : https://plotly.com/python/plotly-express/
"""
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="Olist — Ventes", page_icon="📦", layout="wide")


@st.cache_resource
def session_spark():
    # 1 Go suffit : on ne lit que de petites tables Gold
    return (SparkSession.builder.appName("dashboard-tp1")
            .config("spark.driver.memory", "1g")
            .getOrCreate())


@st.cache_data(ttl=300)
def lire(table):
    """Lit une table Gold et la renvoie sous forme de DataFrame pandas."""
    return session_spark().table(f"lakehouse.gold.{table}").toPandas()


mensuel = lire("ventes_mensuelles").sort_values("mois")
categories = lire("ventes_categorie")
livraison = lire("livraison_etat")

# ---------- Filtre de période (fourni) ----------
st.sidebar.header("Filtres")
mois = mensuel["mois"].tolist()
debut, fin = st.sidebar.select_slider("Période", options=mois, value=(mois[0], mois[-1]))
periode = mensuel[(mensuel["mois"] >= debut) & (mensuel["mois"] <= fin)]

st.title("📦 Olist — Suivi des ventes")
st.caption(f"Période : {debut} → {fin} · Source : couche Gold du Lakehouse")

# ---------- 1. Indicateurs clés ----------
# À COMPLÉTER : quatre indicateurs côte à côte (st.columns(4) puis .metric(...)) :
#   chiffre d'affaires, nombre de commandes et panier moyen sur la période sélectionnée,
#   taux de retard moyen (pondéré par le nombre de commandes livrées de chaque état).


# ---------- 2. Chiffre d'affaires mensuel ----------
# À COMPLÉTER : courbe px.line(periode, x="mois", y="chiffre_affaires"), affichée avec st.plotly_chart


# ---------- 3. Top 10 des catégories ----------
# À COMPLÉTER : barres horizontales des 10 catégories au plus gros chiffre d'affaires


# ---------- 4. Livraison par état ----------
# À COMPLÉTER : barres du taux de retard par état, triées du plus élevé au plus faible


# ---------- 5. Commandes à risque (modèle de la partie 4) ----------
# À COMPLÉTER : lisez gold.predictions_retard avec lire(...), puis :
#   - un curseur st.slider pour choisir le seuil de risque (entre 0 et 1) ;
#   - deux indicateurs : le nombre de commandes signalées, et la part qui était réellement en retard ;
#   - le tableau (st.dataframe) des commandes signalées, de la plus risquée à la moins risquée.

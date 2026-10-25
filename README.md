# Module Data Lakehouse — Environnement des TP

Environnement Lakehouse local, open source, démarré en une commande avec Docker Compose.
Il sert aux TP 1, 2 et 3 du module.

| Service | Rôle | Adresse |
|---|---|---|
| JupyterLab + Spark | Développement (PySpark, dbt, DuckDB, Streamlit, MLflow) | http://localhost:8888 (jeton `lakehouse`) |
| RustFS | Stockage objet compatible S3 | http://localhost:9001 |
| Catalogue Iceberg REST | Référentiel des tables | http://localhost:8181 |
| PostgreSQL | Base source transactionnelle | `localhost:15432` |
| Trino *(profil `sql`/`full`)* | Moteur SQL interactif | http://localhost:8080 |
| Superset *(profil `bi`/`full`)* | Tableaux de bord | http://localhost:8088 |
| Airflow *(profil `orchestration`)* | Orchestration du pipeline (TP 3) | http://localhost:8085 |
| Grafana, Prometheus *(profil `observabilite`)* | Supervision : métriques, tableaux de bord, alertes (TP 3) | http://localhost:3000, http://localhost:9090 |
| Marquez *(profil `observabilite`)* | Observabilité des données : lineage OpenLineage (TP 3) | http://localhost:3001 |
| statsd-exporter, mesure des conteneurs *(profil `observabilite`)* | Métriques d'Airflow et des conteneurs pour Prometheus (TP 3) | — |

## Démarrage rapide

```bash
git clone https://github.com/moeJEE/lakehouse-tp.git
cd lakehouse-tp
cp .env.example .env              # machine de 8 Go : JUPYTER_MEM_LIMIT=3g
docker compose --profile full --profile orchestration --profile observabilite pull
docker compose up -d
docker compose exec jupyter python /opt/lakehouse/scripts/check_env.py
```

Le test doit se terminer par **ENVIRONNEMENT PRÊT**. Les lignes 8 à 13 (Trino, outils du TP 3) restent
en `[--]` tant que leurs profils ne sont pas démarrés.

Le guide complet est dans [`docs/support0_installation.tex`](docs/support0_installation.tex).

## Supports du module

| Support | Document | Notebooks |
|---|---|---|
| Support 0 — Installation | [`docs/support0_installation.tex`](docs/support0_installation.tex) | — |
| TP 1 — Données structurées | [`docs/tp1_donnees_structurees.tex`](docs/tp1_donnees_structurees.tex) | `notebooks/tp1/`, `apps/dashboard_tp1.py` |
| TP 2 — Texte et JSON | [`docs/tp2_texte_json.tex`](docs/tp2_texte_json.tex) | `notebooks/tp2/` (dont le projet dbt), `apps/dashboard_tp2.py` |
| TP 3 — Images, orchestration et BI | [`docs/tp3_images_orchestration_bi.tex`](docs/tp3_images_orchestration_bi.tex) | `notebooks/tp3/` (notebooks, `pipeline/`, DAG Airflow) |

Les données Olist sont dans [`data/olist/`](data/olist/README.md) ; le catalogue photo du TP 3 dans [`data/catalogue/`](data/catalogue/LISEZMOI.md).

## Règle pour les étudiants

Ce dépôt est **en lecture seule** : vous récupérez le code avec `git clone` puis `git pull`, vous ne
poussez rien. Copiez les énoncés dans `notebooks/mon_travail/` (ignoré par Git) et travaillez
uniquement dans ce dossier :

```bash
cp -r notebooks/tp1 apps/dashboard_tp1.py notebooks/mon_travail/
```

Si `git pull` refuse de s'exécuter, un fichier du cours a été modifié par erreur :
`git restore .` le rétablit, sans toucher à `notebooks/mon_travail/`.

## Commandes utiles

| Je veux… | Commande |
|---|---|
| Démarrer | `docker compose up -d` |
| Démarrer avec Trino et Superset | `docker compose --profile full up -d` |
| Démarrer Airflow (TP 3) | `docker compose --profile orchestration up -d` |
| Démarrer la supervision (TP 3) | `docker compose --profile observabilite up -d` puis `docker compose --profile orchestration restart airflow` |
| Arrêter un groupe d'outils | `docker compose --profile full stop` |
| Voir l'état | `docker compose --profile full --profile orchestration --profile observabilite ps -a` |
| Voir les erreurs d'un service | `docker compose logs --tail 50 jupyter` |
| Appliquer une mise à jour du cours | `git pull` puis `docker compose up -d` |
| Arrêter (données conservées) | `docker compose --profile full --profile orchestration --profile observabilite down` |

Pour économiser la mémoire, on ne démarre que les outils de la partie en cours : le tableau
« Quels services pour quelle partie » du Support 0 les récapitule.

> ⚠️ N'utilisez jamais `docker compose down -v` : cette option efface toutes les données du Lakehouse.

## Organisation du dépôt

```
docker-compose.yml     services de l'environnement
.env.example           versions, identifiants locaux, ports, limites mémoire
conf/                  configuration Spark, Trino, dbt, Prometheus, Grafana, Marquez
docker/                images JupyterLab, Superset et Airflow
scripts/               initialisation, test de l'environnement, dépôt des photos, mesure des conteneurs
notebooks/tp1..tp3/    énoncés des TP (à copier, ne pas modifier)
notebooks/mon_travail/ votre travail (ignoré par Git)
apps/                  squelettes des applications
data/olist/            jeux de données des TP 1 et 2 (Olist)
data/catalogue/        photos et fiches produits du TP 3 ; data/modeles/ : modèle ONNX
docs/                  supports du module
```

## Pour l'enseignant

Les images `ghcr.io/moejee/lakehouse-jupyter`, `lakehouse-superset` et `lakehouse-airflow` sont construites
(amd64 + arm64) par le workflow **Images Docker** (onglet *Actions*). Après la première exécution,
rendre les trois paquets publics dans *Packages > Package settings > Change visibility*, pour que les
étudiants puissent les télécharger sans compte.

Les identifiants de `.env.example` sont prévus pour un usage strictement local : tous les ports sont
publiés sur `127.0.0.1`.

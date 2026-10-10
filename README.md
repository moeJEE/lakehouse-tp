# Module Data Lakehouse — Environnement des TP

Environnement Lakehouse local, open source, démarré en une commande avec Docker Compose.
Il sert aux TP 1, 2 et 3 du module.

| Service | Rôle | Adresse |
|---|---|---|
| JupyterLab + Spark | Développement (PySpark, DuckDB, Streamlit, MLflow) | http://localhost:8888 (jeton `lakehouse`) |
| RustFS | Stockage objet compatible S3 | http://localhost:9001 |
| Catalogue Iceberg REST | Référentiel des tables | http://localhost:8181 |
| PostgreSQL | Base source transactionnelle | `localhost:15432` |
| Trino *(profil `sql`/`full`)* | Moteur SQL interactif | http://localhost:8080 |
| Superset *(profil `bi`/`full`)* | Tableaux de bord | http://localhost:8088 |

## Démarrage rapide

```bash
git clone https://github.com/moeJEE/lakehouse-tp.git
cd lakehouse-tp
cp .env.example .env              # machine de 8 Go : JUPYTER_MEM_LIMIT=3g
docker compose --profile full pull
docker compose up -d
docker compose exec jupyter python /opt/lakehouse/scripts/check_env.py
```

Le test doit se terminer par **ENVIRONNEMENT PRÊT**.

Le guide complet est dans [`docs/support0_installation.tex`](docs/support0_installation.tex).

## Commandes utiles

| Je veux… | Commande |
|---|---|
| Démarrer | `docker compose up -d` |
| Démarrer avec Trino et Superset | `docker compose --profile full up -d` |
| Voir l'état | `docker compose ps` |
| Voir les erreurs d'un service | `docker compose logs --tail 50 jupyter` |
| Arrêter (données conservées) | `docker compose --profile full down` |

> ⚠️ N'utilisez jamais `docker compose down -v` : cette option efface toutes les données du Lakehouse.

## Organisation du dépôt

```
docker-compose.yml     services de l'environnement
.env.example           versions, identifiants locaux, ports, limites mémoire
conf/                  configuration Spark et Trino
docker/                images JupyterLab et Superset
scripts/               création des buckets et test de l'environnement
notebooks/ apps/       votre travail (conservé sur votre disque)
data/                  jeux de données des TP (non versionnés)
docs/                  supports du module
```

## Pour l'enseignant

Les images `ghcr.io/moejee/lakehouse-jupyter` et `ghcr.io/moejee/lakehouse-superset` sont construites
(amd64 + arm64) par le workflow **Images Docker** (onglet *Actions*). Après la première exécution,
rendre les deux paquets publics dans *Packages > Package settings > Change visibility*, pour que les
étudiants puissent les télécharger sans compte.

Les identifiants de `.env.example` sont prévus pour un usage strictement local : tous les ports sont
publiés sur `127.0.0.1`.

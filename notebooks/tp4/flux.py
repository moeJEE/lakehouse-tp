"""Fonctions communes aux notebooks du TP 4 (batch et streaming).

Elles servent à préparer et observer les exercices : lancer le rejeu du suivi, remettre un topic
ou un dossier à zéro, suivre une requête en continu. Le cœur du TP (Kafka, Spark Structured
Streaming) s'écrit dans les notebooks.
"""
import json
import os
import subprocess
import sys
import time

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "suivi_colis"
REJEU = "/opt/lakehouse/scripts/rejouer_suivi.py"
CHECKPOINTS = "s3a://lakehouse/checkpoints/tp4"

# Schéma d'un événement du suivi, tel que l'envoie rejouer_suivi.py
SCHEMA_EVENEMENT = ("evenement_id STRING, colis_id STRING, order_id STRING, transporteur STRING, "
                    "categorie STRING, code STRING, horodatage STRING, ville STRING, etat STRING")


def a_completer(quoi):
    """Signale clairement une partie du TP qui n'a pas encore été écrite."""
    raise NotImplementedError(f"⚠️ Cellule à compléter : {quoi}")


# ------------------------------------------------------------------ rejeu
def rejouer(*options, attendre=True):
    """Lance scripts/rejouer_suivi.py avec ces options.

    attendre=True  : attend la fin et renvoie le nombre d'événements envoyés ;
    attendre=False : le rejeu tourne en arrière-plan ; la fonction renvoie le processus,
                     à passer ensuite à fin_du_rejeu().
    """
    commande = [sys.executable, REJEU, *[str(o) for o in options]]
    processus = subprocess.Popen(commande, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if not attendre:
        print("Rejeu lancé en arrière-plan.")
        return processus
    return fin_du_rejeu(processus)


def fin_du_rejeu(processus):
    """Attend la fin d'un rejeu lancé en arrière-plan ; renvoie le nombre d'événements envoyés."""
    sortie, _ = processus.communicate()
    lignes = [l for l in sortie.strip().splitlines() if not l.startswith("%")]   # messages internes du client Kafka
    if processus.returncode != 0:
        print(sortie)
        raise RuntimeError("Le rejeu a échoué : voir le message ci-dessus.")
    print(lignes[0])
    print(lignes[-1])
    return int(lignes[-1].split("Terminé : ")[1].split()[0])


# ------------------------------------------------------------------ remise à zéro
def admin():
    from confluent_kafka.admin import AdminClient
    return AdminClient({"bootstrap.servers": KAFKA})


def supprimer_topic(topic=TOPIC):
    """Supprime le topic s'il existe, et attend que Kafka ait fini de le supprimer."""
    a = admin()
    if topic in a.list_topics(timeout=10).topics:
        for f in a.delete_topics([topic]).values():
            f.result()
    for _ in range(60):
        if topic not in a.list_topics(timeout=10).topics:
            return
        time.sleep(1)
    raise RuntimeError(f"Kafka n'a pas fini de supprimer le topic {topic}.")


def creer_topic(topic=TOPIC, partitions=3):
    """(Re)crée le topic, vide."""
    from confluent_kafka.admin import NewTopic
    supprimer_topic(topic)
    a = admin()                                 # garder le client : ses réponses arrivent après l'appel
    derniere_erreur = None
    for essai in range(30):
        try:
            for f in a.create_topics([NewTopic(topic, num_partitions=partitions,
                                               replication_factor=1)]).values():
                f.result()
            return
        except Exception as e:                  # suppression pas encore propagée : on réessaie
            derniere_erreur = e
            time.sleep(2)
            description = a.list_topics(timeout=10).topics.get(topic)
            if description is not None and len(description.partitions) == partitions:
                return                          # la création a abouti malgré l'erreur
    raise RuntimeError(f"Impossible de créer le topic {topic} : {derniere_erreur}")


def supprimer_groupes(*groupes):
    """Supprime des groupes de consommateurs (et donc leurs offsets mémorisés)."""
    a = admin()
    existants = {g.group_id for g in a.list_consumer_groups().result().valid}
    a_supprimer = [g for g in groupes if g in existants]
    if a_supprimer:
        for f in a.delete_consumer_groups(a_supprimer).values():
            f.result()


def vider_s3(chemin):
    """Supprime tous les objets sous un chemin s3://bucket/prefixe ou s3a://bucket/prefixe."""
    import boto3
    bucket, _, prefixe = chemin.split("://", 1)[1].partition("/")
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefixe.rstrip("/") + "/")
    for page in pages:
        objets = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objets:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objets})


def lister_s3(chemin):
    """Liste les objets sous un chemin s3:// (clé, taille)."""
    import boto3
    bucket, _, prefixe = chemin.split("://", 1)[1].partition("/")
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    cles = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefixe.rstrip("/") + "/"):
        cles += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return cles


def lire_s3(chemin):
    import boto3
    bucket, _, cle = chemin.split("://", 1)[1].partition("/")
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    return s3.get_object(Bucket=bucket, Key=cle)["Body"].read().decode()


def arreter_flux(spark):
    """Arrête toutes les requêtes en continu de la session (utile avant de relancer une cellule)."""
    for q in spark.streams.active:
        q.stop()


# ------------------------------------------------------------------ observation
def suivre(requete, secondes=30, toutes_les=5):
    """Affiche l'avancement d'une requête en continu pendant quelques secondes."""
    fin = time.time() + secondes
    vus = set()
    while time.time() < fin and requete.isActive:
        time.sleep(toutes_les)
        for p in requete.recentProgress:
            if p["batchId"] in vus:
                continue
            vus.add(p["batchId"])
            print(f"micro-lot {p['batchId']:>3} : {p['numInputRows']:>6} lignes lues, "
                  f"traité en {p['durationMs'].get('triggerExecution', 0):>5} ms")
    if requete.exception():
        raise requete.exception()


def offsets_du_checkpoint(chemin):
    """Contenu du dernier fichier d'offsets d'un checkpoint : où en est la lecture de chaque partition."""
    fichiers = [c for c, _ in lister_s3(chemin.replace("s3a://", "s3://") + "/offsets")
                if c.rsplit("/", 1)[1].isdigit()]
    dernier = max(fichiers, key=lambda c: int(c.rsplit("/", 1)[1]))
    contenu = lire_s3("s3://" + chemin.split("://", 1)[1].split("/", 1)[0] + "/" + dernier)
    return int(dernier.rsplit("/", 1)[1]), json.loads(contenu.strip().splitlines()[-1])


# ------------------------------------------------------------------ Airflow (partie 5)
AIRFLOW = os.environ.get("AIRFLOW_URL", "http://airflow:8080")


def airflow(methode, chemin, **kw):
    """Appelle l'API REST d'Airflow (en local, pas de mot de passe : un jeton suffit)."""
    import requests
    jeton = requests.get(f"{AIRFLOW}/auth/token", timeout=30).json()["access_token"]
    r = requests.request(methode, f"{AIRFLOW}/api/v2/{chemin}", timeout=60,
                         headers={"Authorization": f"Bearer {jeton}"}, **kw)
    r.raise_for_status()
    return r.json()


def _executions(dag_id):
    return airflow("GET", f"dags/{dag_id}/dagRuns", params={"limit": 100})["dag_runs"]


def _suivre(dag_id, run):
    print(f"{dag_id} : exécution {run}")
    while True:
        etat = airflow("GET", f"dags/{dag_id}/dagRuns/{run}")["state"]
        if etat in ("success", "failed"):
            print(f"{dag_id} : {etat}")
            return etat
        time.sleep(10)


def lancer_dag(dag_id):
    """Active le DAG si besoin, attend la fin des exécutions en cours, puis en déclenche une et la suit.

    À l'activation d'un DAG planifié, Airflow lance aussitôt l'exécution de la dernière période :
    on la suit, et on ne déclenche une exécution manuelle que s'il n'y en a pas eu.
    """
    import requests
    for _ in range(30):                     # Airflow lit un nouveau DAG en 30 s environ
        try:
            dag = airflow("GET", f"dags/{dag_id}")
            break
        except requests.HTTPError:
            time.sleep(10)
    else:
        raise RuntimeError(f"Airflow ne trouve pas le DAG {dag_id} : vérifiez qu'il est bien copié.")
    # Airflow n'exécute ici qu'une tâche à la fois : les DAG du TP 3 encore actifs passeraient devant.
    for autre in airflow("GET", "dags", params={"paused": "false", "limit": 100})["dags"]:
        if autre["dag_id"] != dag_id:
            airflow("PATCH", f"dags/{autre['dag_id']}", json={"is_paused": True})
            print(f"{autre['dag_id']} mis en pause (pour laisser la place à {dag_id})")
    if dag["is_paused"]:
        deja = {r["dag_run_id"] for r in _executions(dag_id)}
        airflow("PATCH", f"dags/{dag_id}", json={"is_paused": False})
        print(f"{dag_id} activé")
        for _ in range(9):                   # exécution automatique de la dernière heure ?
            time.sleep(10)
            nouvelles = [r for r in _executions(dag_id) if r["dag_run_id"] not in deja]
            if nouvelles:
                return _suivre(dag_id, nouvelles[0]["dag_run_id"])
    while any(r["state"] in ("queued", "running") for r in _executions(dag_id)):
        time.sleep(10)
    run = airflow("POST", f"dags/{dag_id}/dagRuns", json={"logical_date": None})["dag_run_id"]
    return _suivre(dag_id, run)

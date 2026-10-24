"""Mesure le processeur et la mémoire de chaque conteneur, pour Prometheus (TP 3, partie 5).

Remplace cAdvisor, qui ne reconnaît pas les conteneurs avec Docker Desktop (Windows, Mac).
Le service interroge l'API de Docker, comme la commande « docker stats », et publie sur
http://<service>:8080/metrics les mêmes métriques que cAdvisor :
    container_memory_working_set_bytes{name="..."}   mémoire utilisée (octets)
    container_cpu_usage_seconds_total{name="..."}    temps processeur cumulé (secondes)
    container_last_seen{name="..."}                  horodatage de la dernière mesure
Bibliothèque standard uniquement.
"""
import http.client
import json
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SOCKET_DOCKER = "/var/run/docker.sock"


class ConnexionUnix(http.client.HTTPConnection):
    def __init__(self):
        super().__init__("localhost", timeout=20)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(20)
        self.sock.connect(SOCKET_DOCKER)


def docker(chemin):
    cnx = ConnexionUnix()
    try:
        cnx.request("GET", chemin)
        return json.loads(cnx.getresponse().read())
    finally:
        cnx.close()


def mesurer(conteneur):
    nom = conteneur["Names"][0].lstrip("/")
    s = docker(f"/containers/{conteneur['Id']}/stats?stream=false&one-shot=true")
    memoire = s.get("memory_stats", {})
    details = memoire.get("stats", {})
    # « working set » : mémoire utilisée moins le cache de fichiers récupérable (comme cAdvisor)
    cache = details.get("inactive_file", details.get("total_inactive_file", 0))
    utilisee = max(memoire.get("usage", 0) - cache, 0)
    cpu = s.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0) / 1e9
    return nom, utilisee, cpu


def metriques():
    conteneurs = docker("/containers/json")
    with ThreadPoolExecutor(max_workers=16) as pool:
        mesures = list(pool.map(mesurer, conteneurs))
    maintenant = time.time()
    lignes = ["# TYPE container_memory_working_set_bytes gauge",
              "# TYPE container_cpu_usage_seconds_total counter",
              "# TYPE container_last_seen gauge"]
    for nom, memoire, cpu in mesures:
        lignes += [f'container_memory_working_set_bytes{{name="{nom}"}} {memoire}',
                   f'container_cpu_usage_seconds_total{{name="{nom}"}} {cpu:.3f}',
                   f'container_last_seen{{name="{nom}"}} {maintenant:.0f}']
    return "\n".join(lignes) + "\n"


class Gestionnaire(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        try:
            corps, code = metriques().encode(), 200
        except Exception as e:                       # Docker injoignable : on le dit, sans planter
            corps, code = f"# erreur : {e}\n".encode(), 500
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(corps)

    def log_message(self, *args):                    # pas de ligne de journal à chaque collecte
        pass


if __name__ == "__main__":
    print("mesure des conteneurs : http://0.0.0.0:8080/metrics", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Gestionnaire).serve_forever()

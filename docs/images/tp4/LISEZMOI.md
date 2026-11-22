# Captures d'écran du TP4

Déposez ici les captures du support `tp4_batch_streaming.tex`, au format PNG et sous le nom exact indiqué.
Tant qu'un fichier manque, le PDF affiche à sa place un cadre « Capture à ajouter ».
Les numéros de cellule sont les mêmes dans l'énoncé et dans le corrigé.

| Fichier | Contenu attendu | Où faire la capture |
|---|---|---|
| `tp4_01_micro_lots.png` | Partie 1 : le nombre d'événements par code, mis à jour à chaque micro-lot. | `01_du_fichier_au_flux` : sortie de la cellule 6, des tableaux « après 10 s » à « après 20 s » |
| `tp4_02_messages.png` | Partie 2 : les messages du topic `suivi_colis` dans Kafka UI. | `02_kafka`, après la cellule 3 : Kafka UI (http://localhost:8090), *Topics* > `suivi_colis` > onglet *Messages*, tri *Newest* |
| `tp4_02_consumers.png` | Partie 2 : les groupes de consommateurs et leur retard. | `02_kafka`, après la cellule 7 : Kafka UI, menu *Consumers* |
| `tp4_03_panne.png` | Partie 3 : la panne, la reprise et la vérification de la table Bronze. | `03_bronze_streaming` : sorties des cellules 5 à 7 |
| `tp4_04_watermark.png` | Partie 4 : les incidents par jour, en flux avec 12 h d'attente, comparés au batch. | `04_temps_fenetres` : cellule 6 et sa sortie |
| `tp4_05_airflow.png` | Partie 5 : les exécutions du DAG `suivi_incidents`. | Airflow (http://localhost:8085), DAG `suivi_incidents`, vue *Grid* et onglet *Runs*, après la cellule 8 |
| `tp4_05_bilan.png` | Partie 5 : le bilan des façons de calculer les incidents. | `05_batch_ou_streaming` : cellule 12 et sa sortie |

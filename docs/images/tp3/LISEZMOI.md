# Captures d'écran du TP3

Déposez ici les captures du support `tp3_images_orchestration_bi.tex`, au format PNG et sous le nom exact indiqué.
Tant qu'un fichier manque, le PDF affiche à sa place un cadre « Capture à ajouter ».

| Fichier | Contenu attendu | Où faire la capture |
|---|---|---|
| `tp3_01_doublons.png` | Partie 1 : un produit envoyé avec deux photos, la même recadrée. | `01_images_catalogue` : sortie de la cellule 15 du corrigé (14 de l'énoncé), deuxième paire affichée (`_v2.jpg`, la photo recadrée) |
| `tp3_01_similaires.png` | Partie 1 : une photo et les cinq photos les plus semblables selon MobileNet. | `01_images_catalogue` : sortie de la cellule 25 du corrigé (23 de l'énoncé), une rangée de six photos |
| `tp3_02_graph.png` | Partie 2 : le DAG catalogue_photos dans Airflow, vue Graph. | `02_pipeline_airflow`, après la cellule 18 du corrigé (17 de l'énoncé) : Airflow, DAG `catalogue_photos`, onglet *Graph*, exécution réussie |
| `tp3_02_gantt.png` | Partie 2 : la durée de chaque tâche, vue Gantt, pour le lot 1 et le lot 2. | `02_pipeline_airflow`, après la cellule 20 du corrigé (19 de l'énoncé), une fois le DAG relancé sur le lot 2 : onglet *Gantt* |
| `tp3_03_fichiers.png` | Partie 3 : le nombre de fichiers avant et après le compactage. | `03_optimiser` : sortie de la cellule 21 du corrigé (19 de l'énoncé), nombre de fichiers avant et après `maintenance_iceberg` |
| `tp3_05_grafana.png` | Partie 5 : le tableau de bord Grafana pendant une exécution du DAG. | `05_surveiller_observer`, pendant la cellule 5 (exécution du DAG) : Grafana, tableau *Lakehouse — supervision* |
| `tp3_05_lineage.png` | Partie 5 : le lineage de gold.photos_controle dans Marquez. | `05_surveiller_observer`, question 4 (cellule 19 du corrigé, 16 de l'énoncé) : Marquez, recherche `photos_controle`, vue du graphe |
| `tp3_dashboard.png` | Le tableau de bord Superset attendu. | `04_trino_superset`, étape 5 (cellule 14 du corrigé, 13 de l'énoncé), une fois le tableau de bord construit : Superset, tableau *Olist — Satisfaction et catalogue* |

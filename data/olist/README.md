# Données Olist

**Brazilian E-Commerce Public Dataset by Olist** : environ 100 000 commandes passées entre
2016 et 2018 sur des marketplaces brésiliennes, avec les produits, vendeurs, clients,
paiements et avis.

- Source : <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>
- Licence : CC BY-NC-SA 4.0 (usage non commercial, attribution, partage dans les mêmes conditions)
- Données fournies par Olist. Elles sont redistribuées ici sans modification, pour un usage pédagogique.

## Fichiers attendus dans ce dossier

| Fichier | Utilisé dans |
|---|---|
| `olist_orders_dataset.csv` | TP 1 (chargé dans PostgreSQL) |
| `olist_order_items_dataset.csv` | TP 1 (chargé dans PostgreSQL) |
| `olist_order_payments_dataset.csv` | TP 1 (chargé dans PostgreSQL) |
| `olist_customers_dataset.csv` | TP 1 (fichier CSV) |
| `olist_products_dataset.csv` | TP 1 (fichier CSV) |
| `olist_sellers_dataset.csv` | TP 1 (fichier CSV) |
| `product_category_name_translation.csv` | TP 1 (fichier CSV) |
| `olist_order_reviews_dataset.csv` | TP 2 et TP 3 |

`olist_geolocation_dataset.csv` (61 Mo) n'est pas utilisé et n'est pas versionné.

## Pour l'enseignant : ajouter les fichiers au dépôt

1. Télécharger l'archive depuis la page Kaggle (bouton *Download*, compte Kaggle requis).
2. Décompresser les 8 fichiers ci-dessus dans ce dossier.
3. `git add data/olist && git commit -m "Données Olist" && git push`

Les étudiants récupèrent ensuite les données avec le dépôt, sans compte Kaggle.

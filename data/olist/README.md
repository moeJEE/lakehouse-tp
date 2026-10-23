# Données Olist

**Brazilian E-Commerce Public Dataset by Olist** : environ 100 000 commandes passées entre
2016 et 2018 sur des marketplaces brésiliennes, avec les produits, vendeurs, clients,
paiements et avis.

- Source : <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>
- Licence : CC BY-NC-SA 4.0 (usage non commercial, attribution, partage dans les mêmes conditions)
- Données fournies par Olist. Les fichiers de ce dossier sont une copie publiée sur GitHub en 2020
  ([spdrio/Brazilian-E-Commerce-Public-Dataset-by-Olist](https://github.com/spdrio/Brazilian-E-Commerce-Public-Dataset-by-Olist)),
  redistribuée ici pour un usage pédagogique, sans autre modification.
- Les volumes des tables du TP 1 sont identiques à la version actuelle de Kaggle. La table des avis
  (`order_reviews`, 100 000 lignes) correspond à une version plus ancienne : la version actuelle de
  Kaggle en compte 99 224, après suppression de doublons.

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
| `suivi/mois=AAAA-MM/suivi-AAAA-MM.json` | TP 2 : messages de suivi des colis (voir ci-dessous) |

`olist_geolocation_dataset.csv` (61 Mo) n'est pas utilisé et n'est pas versionné.

## Messages de suivi des colis (`suivi/`)

Un message JSON par ligne et par colis, un fichier par mois (98 249 messages). Ils sont **reconstruits** par
`scripts/generer_suivi.py` à partir des vraies dates de remise au transporteur et de livraison des
commandes Olist ; les transporteurs, les lieux de transit et les incidents sont **simulés**, ainsi que deux
défauts volontaires (messages renvoyés deux fois, messages tronqués). Le résultat est identique à chaque
exécution. Pour régénérer les fichiers, depuis un terminal JupyterLab :

```bash
python /opt/lakehouse/scripts/generer_suivi.py --local data/olist/suivi
```

## Mettre à jour les données

Pour utiliser la version actuelle de Kaggle : télécharger l'archive depuis la page Kaggle
(compte requis), remplacer les fichiers de ce dossier, puis `git add data/olist && git commit && git push`.

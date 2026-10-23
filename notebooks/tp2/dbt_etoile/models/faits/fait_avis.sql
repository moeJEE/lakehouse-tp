-- Fait des avis. Grain : l'avis d'une commande (review_id, order_id).
-- À COMPLÉTER : joindre silver.avis à silver.orders. Colonnes attendues :
--   review_id, order_id, date_avis_id (date de création de l'avis, entier AAAAMMJJ),
--   customer_id, note, est_negatif, a_commentaire (au moins un mot), nb_mots,
--   en_retard, delai_livraison_jours
-- Modèle à suivre : models/faits/fait_ventes.sql
select
    a.review_id,
    a.order_id
from {{ source('silver', 'avis') }} as a

-- Fait des livraisons. Grain : un colis (colis_id), décrit par le flux JSON du transporteur.
-- À COMPLÉTER : joindre silver.suivi_colis à silver.orders. Colonnes attendues :
--   colis_id, order_id, date_expedition_id (date de remise au transporteur, AAAAMMJJ),
--   customer_id, transporteur, service, nb_evenements, nb_incidents, premier_incident,
--   duree_transit_h, en_retard
select
    c.colis_id,
    c.order_id
from {{ source('silver', 'suivi_colis') }} as c

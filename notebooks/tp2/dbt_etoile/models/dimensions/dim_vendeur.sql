-- Dimension vendeur : une ligne par vendeur.
select
    seller_id,
    seller_city  as ville,
    seller_state as etat
from {{ source('silver', 'sellers') }}

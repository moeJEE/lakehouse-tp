-- Test « singulier » : renvoie les lignes en erreur. Le test réussit s'il ne renvoie rien.
-- Ici : un même article de commande ne doit apparaître qu'une fois (respect du grain).
select order_id, order_item_id, count(*) as nb
from {{ ref('fait_ventes') }}
group by order_id, order_item_id
having count(*) > 1

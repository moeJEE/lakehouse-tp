-- Fait des ventes. Grain : un article d'une commande (order_id, order_item_id).
-- Les mesures (prix, frais de port) s'additionnent ; les clés renvoient aux dimensions.
select
    i.order_id,
    cast(i.order_item_id as int)                                         as order_item_id,
    cast(date_format(o.order_purchase_timestamp, 'yyyyMMdd') as int)     as date_achat_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    i.price                                                              as prix,
    i.freight_value                                                      as frais_port,
    o.order_status                                                       as statut_commande,
    o.en_retard
from {{ source('silver', 'order_items') }} as i
join {{ source('silver', 'orders') }} as o on o.order_id = i.order_id

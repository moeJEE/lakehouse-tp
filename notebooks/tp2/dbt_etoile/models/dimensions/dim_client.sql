-- Dimension client : une ligne par client (customer_id).
select
    customer_id,
    customer_unique_id,
    customer_city  as ville,
    customer_state as etat
from {{ source('silver', 'customers') }}

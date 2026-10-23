-- Dimension produit : une ligne par produit.
-- À COMPLÉTER, à partir de la source silver.products. Colonnes attendues :
--   product_id, categorie (catégorie en anglais), categorie_pt (catégorie en portugais),
--   poids_g, volume_cm3 (longueur x hauteur x largeur), nb_photos
select
    product_id
from {{ source('silver', 'products') }}

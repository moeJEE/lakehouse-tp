-- Un nombre d'incidents ne peut pas être négatif (piège de F.size sur une liste nulle).
select colis_id, nb_incidents
from {{ ref('fait_livraisons') }}
where nb_incidents < 0

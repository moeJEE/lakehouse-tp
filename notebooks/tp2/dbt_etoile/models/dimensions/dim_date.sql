-- Dimension date : une ligne par jour, de 2016 à 2018.
-- La clé date_id est un entier AAAAMMJJ (20180315), facile à lire et à joindre.
with jours as (
    select explode(sequence(to_date('2016-01-01'), to_date('2018-12-31'), interval 1 day)) as jour
)
select
    cast(date_format(jour, 'yyyyMMdd') as int) as date_id,
    jour                                       as date,
    year(jour)                                 as annee,
    quarter(jour)                              as trimestre,
    month(jour)                                as mois,
    date_format(jour, 'yyyy-MM')               as annee_mois,
    dayofweek(jour)                            as jour_semaine,   -- 1 = dimanche, 7 = samedi
    dayofweek(jour) in (1, 7)                  as est_weekend
from jours

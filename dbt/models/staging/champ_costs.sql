{{ config(materialized="view", schema="staging") }}

select
    case
        when unit_name = 'Akali'
        then 'K/DA Akali'
        when unit_name = 'TrueDamage'
        then 'True Damage Akali'
        else unit_name
    end as champ_name,
    case
        when unit_rarity.unit_rarity = 0
        then 1
        when unit_rarity.unit_rarity = 1
        then 2
        when unit_rarity.unit_rarity = 2
        then 3
        when unit_rarity.unit_rarity = 6
        then 5
        else unit_rarity.unit_rarity
    end as champ_cost
from {{ source("staging", "unit_rarity") }}

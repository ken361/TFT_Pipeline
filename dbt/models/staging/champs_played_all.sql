{{ config(materialized="view", schema="staging") }}

select
    match_id,
    placement,
    case
        when unit_name = 'Akali'
        then 'K/DA Akali'
        when unit_name = 'TrueDamage'
        then 'True Damage Akali'
        else unit_name
    end as champ_name,
    unit_tier as champ_tier,
    unit_items.element as champ_items
from {{ source("staging", "units_played_all") }}
left join unnest(unit_items.list) as unit_items on true

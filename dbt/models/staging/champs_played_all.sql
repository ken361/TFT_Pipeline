{{ config(materialized="view", schema="staging") }}

select
    match_id,
    placement,
    unit_name as champ_id,
    unit_tier as champ_tier,
    unit_items.element as champ_items
from {{ source("staging", "units_played_all") }}
left join
    unnest(unit_items.list) as unit_items on true
    -- Not changing the granularity of the table, but maybe shouldn't unnest in
    -- staging environment
    
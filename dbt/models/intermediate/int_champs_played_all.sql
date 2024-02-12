{{ config(materialized="view", schema="intermediate") }}

select
    match_id,
    placement,
    struct(
        champ_name as name,
        champ_tier as tier,
        array_agg(champ_items ignore nulls) as items
    ) as champion
from {{ ref("champs_played_all") }}
where champ_items != 'Empty Bag' or champ_items is null
group by
    match_id,
    placement,
    champ_name,
    champ_tier

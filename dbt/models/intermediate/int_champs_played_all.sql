{{ config(materialized="view", schema="intermediate") }}

select
    main.match_id, 
    main.placement, 
    name.name as champion,
    name.id as champ_id,
    max(main.champ_tier) as tier,
from {{ ref("champs_played_all") }} main
inner join {{ ref("champ_id_to_name") }} name on main.champ_id = name.id
group by
    match_id,
    placement,
    champion,
    champ_id

-- No longer require item data with champion data in the same table
/*
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
*/
    

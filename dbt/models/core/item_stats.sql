{{ config(materialized="table", schema="core") }}
/*
select distinct 
    champ_items,
    case
        when LEFT(champ_items, 4) = 'Ornn' THEN 'Ornn Item'
        when LEFT(champ_items, 7) = 'Shimmer' THEN 'Ornn Item'
        when RIGHT(champ_items, 7) = 'Radiant' THEN 'Radiant Item'
        when RIGHT(champ_items, 6) = 'Emblem' THEN 'Emblem'
        else 'Normal Item'
    end as item_category,
    round(avg(placement), 2) as avg_placement,
    round(
        sum(case when placement < 5 then 1 else 0 end) / count(champ_items) * 100, 2
    ) as top_4_pct,
    round(
        sum(case when placement = 1 then 1 else 0 end) / count(champ_items) * 100, 2
    ) as top_1_pct
from {{ ref("champs_played_all") }}
where champ_items is not null
    and champ_items not in ('Empty Bag', 'Unstable Concoction')
group by champ_items
*/
select
    item.name as item,
    item.item_type,
    round(avg(placement), 2) as avg_placement,
    round(
        sum(case when placement < 5 then 1 else 0 end) / count(item.name) * 100, 2
    ) as top_4_pct,
    round(
        sum(case when placement = 1 then 1 else 0 end) / count(item.name) * 100, 2
    ) as top_1_pct
from {{ ref("champs_played_all") }} main
inner join {{ ref("int_item_id_to_name") }} item on main.champ_items = item.id
group by item, item_type

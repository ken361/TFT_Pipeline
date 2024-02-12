{{ config(materialized="view", schema="intermediate") }}

with
    champ_item_cte as (
        select
            champ_name as champion,
            champ_items as item,
            count(champ_items) as equip_count,
            avg(placement) as avg_placement
        from {{ ref("champs_played_all") }}
        where
            champ_items is not null
            and champ_items != 'Empty Bag'
            and champ_items != 'Unstable Concoction'
        group by champion, item
    ),
    ranking_cte as (
        select
            champion,
            item,
            equip_count,
            avg_placement,
            dense_rank() over (partition by champion order by equip_count desc) as rank
        from champ_item_cte
    )
select champion, item, equip_count, round(avg_placement, 2) as avg_placement
from ranking_cte
where
    rank in (1, 2, 3) and equip_count > 3

    /* Maybe just remove avg_placement, or just filter for avg < 6 or smth earlier*/
    /* Maybe also just use this as the finalized item table*/
    

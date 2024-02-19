{{ config(materialized="table", schema="core") }}

with
    champ_play_count_cte as (
        select champion, champ_id, count(champion) as times_played
        from {{ ref("int_champs_played_all") }}
        group by champion, champ_id
    ),
    champ_item_cte as (
        select
            count_cte.champion as champion,
            item.name as item,
            count(champ_items) as times_equipped,
            avg(placement) as avg_placement,
            round(count(champ_items) / times_played * 100, 2) as equip_pct
        from {{ ref("champs_played_all") }} main
        inner join
            {{ ref("int_item_id_to_name") }} item on main.champ_items = item.id
        inner join
            champ_play_count_cte count_cte on main.champ_id = count_cte.champ_id
        group by champion, item, times_played
    ),
    ranking_cte as (
        select
            champion,
            item,
            times_equipped,
            equip_pct,
            avg_placement,
            rank() over (
                partition by champion order by times_equipped desc
            ) as rank
        from champ_item_cte
    )
select
    champion, item, round(avg_placement, 2) as avg_placement, equip_pct, times_equipped
from ranking_cte
where rank <= 3 and times_equipped > 5

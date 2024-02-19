{{ config(materialized="view", schema="intermediate") }}

with
    item_names_cte as (
        select *
        from {{ ref("item_id_to_name") }}
        where
            name != ""
            and item_type != "Other"
    ),
    champ_item_cte as (
        select
            main.champ_id as champion,
            item_names_cte.name as item,
            count(champ_items) as times_equipped,
            avg(placement) as avg_placement
        from {{ ref("champs_played_all") }} main
        inner join item_names_cte
            on main.champ_items = item_names_cte.id
        where
            champ_items is not null
            and champ_items != 'Empty Bag'
            and champ_items != 'Unstable Concoction'
        group by champion, item, name
    ),
    ranking_cte as (
        select
            champion,
            item,
            times_equipped,
            avg_placement,
            dense_rank() over (partition by champion order by times_equipped desc) as rank
        from champ_item_cte
    )
select champion, item, round(avg_placement, 2) as avg_placement, times_equipped
from ranking_cte
where rank in (1, 2, 3) and times_equipped > 3

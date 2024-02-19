{{ config(materialized="table", schema="core") }}

with
    trait_avg_placement_cte as (
        select trait, avg(placement) as avg_placement,
        from {{ ref("int_traits_all") }}
        where style in ('Silver', 'Gold', 'Prismatic')
        group by trait
    ),
    trait_aug_placement_cte as (
        select
            trait, augment, avg(placement) as avg_placement, count(*) as times_played,
        from {{ ref("int_trait_augment_combinations") }}
        group by trait, augment
    ),
    full_table_cte as (
        select
            combo_avg.trait,
            combo_avg.augment,
            round(trait_avg.avg_placement, 2) as trait_avg_placement,
            round(combo_avg.avg_placement, 2) as combo_avg_placement,
            round(
                trait_avg.avg_placement - combo_avg.avg_placement, 2
            ) as avg_placement_change,
            combo_avg.times_played,
        from trait_aug_placement_cte combo_avg
        inner join
            trait_avg_placement_cte trait_avg on combo_avg.trait = trait_avg.trait
    )
select *
from full_table_cte
where times_played >= 30 and avg_placement_change > 0
order by avg_placement_change desc

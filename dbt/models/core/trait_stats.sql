{{ config(materialized="table", schema="core") }}

select
    trait,
    style,
    round(avg(placement), 2) as avg_placement,
    round(
        sum(case when placement < 5 then 1 else 0 end) / count(trait_with_style) * 100,
        2
    ) as top_4_pct,
    round(
        sum(case when placement = 1 then 1 else 0 end) / count(trait_with_style) * 100,
        2
    ) as top_1_pct,
    count(trait_with_style) as times_played
from {{ ref("int_traits_all") }}
where style != 'No style'
group by trait_with_style, trait, style
order by trait_with_style

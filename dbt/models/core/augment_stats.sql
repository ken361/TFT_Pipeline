{{ config(materialized="table", schema="core") }}

select
    augment,
    round(avg(placement), 2) as avg_placement,
    round(
        sum(case when placement < 5 then 1 else 0 end) / count(augment) * 100, 2
    ) as top_4_pct,
    round(
        sum(case when placement < 2 then 1 else 0 end) / count(augment) * 100, 2
    ) as top_1_pct,
    count(augment) as times_played
from {{ ref("augments_played_all") }}
group by augment
order by times_played desc

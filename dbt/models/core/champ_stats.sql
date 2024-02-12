{{ config(materialized="table", schema="core") }}

select
    champion.name as champion,
    champ_cost as cost,
    cast(round(avg(champion.tier), 0) as int64) as avg_tier,
    round(avg(placement), 2) as avg_placement,
    round(
        sum(case when placement < 5 then 1 else 0 end) / count(champion) * 100, 2
    ) as top_4_pct,
    round(
        sum(case when placement < 2 then 1 else 0 end) / count(champion) * 100, 2
    ) as top_1_pct,
    count(champion) as times_played
from {{ ref("int_champs_played_all") }} stats
inner join {{ ref("champ_costs") }} costs on stats.champion.name = costs.champ_name
group by champion, cost
order by
    avg_placement

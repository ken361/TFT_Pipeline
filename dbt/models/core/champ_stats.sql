{{ config(materialized="table", schema="core") }}

{%- call statement('get_num_matches', fetch_result=True) -%}
      SELECT COUNT(DISTINCT match_id) FROM {{ ref("int_champs_played_all") }}
{%- endcall -%}

{%- set num_matches = load_result('get_num_matches')['data'][0][0] -%}

select
    champion,
    champ_cost as cost,
    cast(round(avg(tier), 0) as int64) as avg_tier,
    round(avg(placement), 2) as avg_placement,
    round(
        sum(case when placement < 5 then 1 else 0 end) / count(champion) * 100, 2
    ) as top_4_pct,
    round(
        sum(case when placement = 1 then 1 else 0 end) / count(champion) * 100, 2
    ) as top_1_pct,
    round(count(champion) / ({{ num_matches }} * 8) * 100, 2) as pick_pct,
    count(champion) as times_played
from {{ ref("int_champs_played_all") }} stats
inner join {{ ref("champ_costs") }} costs on stats.champ_id = costs.champ_id
group by champion, cost
order by
    avg_placement
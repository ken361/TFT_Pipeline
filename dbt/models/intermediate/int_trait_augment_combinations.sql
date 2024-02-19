{{ config(materialized="view", schema="intermediate") }}

select trait.match_id, trait.placement, trait.trait, trait.style, aug.augment,
from {{ ref("int_traits_all") }} trait
inner join
    {{ ref("int_augments_played_all") }} aug
    on trait.match_id = aug.match_id
    and trait.placement = aug.placement
where trait.style in ('Silver', 'Gold', 'Prismatic')  -- All 3 styles weighted the same for now

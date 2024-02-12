{{ config(materialized="view", schema="staging") }}

select *
from {{ source("staging", "traits_played_all") }}

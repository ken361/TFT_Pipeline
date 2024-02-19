{{ config(materialized="view", schema="staging") }}

select *
from {{ source("staging", "trait_id_to_name") }}

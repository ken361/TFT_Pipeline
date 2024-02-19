{{ config(materialized="view", schema="staging") }}

select *
from {{ source("staging", "item_id_to_name") }}

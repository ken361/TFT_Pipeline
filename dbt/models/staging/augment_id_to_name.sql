{{ config(materialized="view", schema="staging") }}

select *
from {{ source("staging", "augment_id_to_name") }}

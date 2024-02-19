{{ config(materialized="view", schema="staging") }}

select match_id, placement, augments.element as augment_id
from {{ source("staging", "augments_played_all") }}, unnest(augments.list) as augments

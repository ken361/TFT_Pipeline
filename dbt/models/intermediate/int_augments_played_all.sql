{{ config(materialized="view", schema="intermediate") }}

select match_id, placement, idn.name as augment
from {{ ref("augments_played_all") }} data
inner join {{ ref("augment_id_to_name") }} idn on data.augment_id = idn.id

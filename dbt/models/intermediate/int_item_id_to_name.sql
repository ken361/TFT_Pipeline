{{ config(materialized="view", schema="intermediate") }}

select *
from {{ ref("item_id_to_name") }}
where
    name != ""
    and item_type != "Other"
    and left(name, 1) != "@"
    and id NOT LIKE '%Debug%'
    and id NOT LIKE '%DJ_Mode%'
    and id NOT LIKE '%Grant%'
    and name != "Unusable Slot"

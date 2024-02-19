{{ config(materialized="view", schema="staging") }}

select
    id,
    case
        when id = 'TFT10_Akali_TrueDamage'
        then 'Akali - True Damage'
        when id = 'TFT10_Akali'
        then 'Akali - K/DA'
        else name
    end as name
from {{ source("staging", "champ_id_to_name") }}

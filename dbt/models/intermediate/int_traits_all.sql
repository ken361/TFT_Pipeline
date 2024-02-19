{{ config(materialized="view", schema="intermediate") }}

with
    traits_id_data as (
        select match_id, placement, trait_names.element as trait, ofst
        from
            {{ ref("traits_played_all") }} main, unnest(trait_names.list) as trait_names
        with
        offset as ofst
    ),
    traits_style_data as (
        select match_id, placement, trait_styles.element as style, ofst
        from 
            {{ ref("traits_played_all") }}, unnest(trait_styles.list) as trait_styles
        with
        offset as ofst
    )

select
    a.match_id,
    a.placement,
    c.name as trait,
    case
        when b.style = 0
        then 'No style'
        when b.style = 1
        then 'Bronze'
        when b.style = 2
        then 'Silver'
        when b.style = 3
        then 'Gold'
        when b.style = 4
        then 'Prismatic'
        else '-'
    end as style,
    case
        when b.style = 0
        then concat(a.trait, '0')
        when b.style = 1
        then concat(a.trait, '1')
        when b.style = 2
        then concat(a.trait, '2')
        when b.style = 3
        then concat(a.trait, '3')
        when b.style = 4
        then concat(a.trait, '4')
        else a.trait
    end as trait_with_style
from traits_id_data a
inner join
    traits_style_data b
    on a.match_id = b.match_id
    and a.placement = b.placement
    and a.ofst = b.ofst
inner join {{ ref("trait_id_to_name") }} c on a.trait = c.id

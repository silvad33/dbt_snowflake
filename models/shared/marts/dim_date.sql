{{
  config(
    materialized = 'view',
    tags = ['dim'],
    )
}}

with final as (

    select

        *

    from {{ ref('stg_date') }}
)

select * from final
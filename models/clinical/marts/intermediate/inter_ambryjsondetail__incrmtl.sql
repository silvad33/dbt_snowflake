{{
  config(
    materialized = 'incremental',
    unique_key = 'ambryTestDetailKey'
    )
}}

with new_record_ref as (
    {#New records in the stage that have not been loaded into the incremental table yet#}
    select *  from {{ ref('stg_azure__ambryjsondetail') }}

    {% if is_incremental() %}
    where recordCreateDateTime > (select max(recordCreateDateTime) from {{this}})
    {% endif %}
),

-- (re)process all records
new_records as (

select * from new_record_ref

)

select * from new_records
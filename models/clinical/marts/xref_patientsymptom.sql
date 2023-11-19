{{
  config(
    materialized = 'table',
    tags = ['dim'],
    )
}}

with ref as (

    select distinct
    patientSymptomKey,
    patientKey,
    symptomKey,
    patientSymptomOnsetAge,
    recordCreateDateTime as auditCreatedAt,
    recordCreatedByUser as auditCreatedBy
    
    from {{ ref('inter_ambryjsondetail__incrmtl') }}

    where symptomKey is not null

),

final as (

    select * from ref

)

select * from final

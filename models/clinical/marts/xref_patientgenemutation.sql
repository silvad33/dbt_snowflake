{{
  config(
    materialized = 'table',
    tags = ['dim'],
    )
}}

with ref as (

    select distinct
    patientOtherGeneMutationKey as patientGeneMutationKey,
    patientKey,
    otherGeneMutationKey as geneMutationKey,
    recordCreateDateTime as auditCreatedAt,
    recordCreatedByUser as auditCreatedBy
    
    from {{ ref('inter_ambryjsondetail__incrmtl') }}

    where otherGeneMutationKey is not null

),

final as (

    select * from ref

)

select * from final

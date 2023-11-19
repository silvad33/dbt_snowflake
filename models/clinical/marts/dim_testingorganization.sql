{{
  config(
    materialized = 'table',
    tags = ['dim'],
    )
}}

with final as (

    select distinct
    testingOrganizationKey,
    testingOrganizationFullName,
    testingOrganizationName,
    testingOrganizationNumberNK,
    recordCreateDateTime as auditCreatedAt,
    recordCreatedByUser as auditCreatedBy      

    from {{ ref('inter_ambryjsondetail__incrmtl') }}

    where testingOrganizationKey is not null

)

select * from final
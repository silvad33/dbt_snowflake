{{
  config(
    materialized = 'table',
    tags = ['dim'],
    )
}}

with final as (

    select distinct
    testingOrganizationAddressKey,
    testingOrganizationStreetAddress,
    testingOrganizationCity,
    testingOrganizationState,
    testingOrganizationZipCode,
    testingOrganizationCountry,
    recordCreateDateTime as auditCreatedAt,
    recordCreatedByUser as auditCreatedBy       

    from {{ ref('inter_ambryjsondetail__incrmtl') }}

    where testingOrganizationAddressKey is not null

)

select * from final
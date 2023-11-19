{{
  config(
    materialized = 'table',
    tags = ['dim'],
    )
}}

with primaryclinicianref as (

    select distinct
    clinicianKey,
    clinicianNPINumberNK,
    max(clinicianName) as clinicianName,
    max(clinicianEmail) as clinicianEmail,
    max(clinicianPhone) as clinicianPhone,
    max(recordCreateDateTime) as auditCreatedAt,
    max(recordCreatedByUser) as auditCreatedBy
    
    from {{ ref('inter_ambryjsondetail__incrmtl') }}

    where clinicianKey is not null
    /*This next block of logic picks the latest values based on test date, following the latest values from the source to eliminate potential duplication issues. 
        Added max() wrapper around non-key values to pull only one value per clinicianKey. Source data needs to be corrected in some scenarios.
        For undoing deletions from previous version: patientKey in first concat, maxPatientKey in second concat, max(patientKey) as maxPatientKey in innermost query.
    */
    and concat(clinicianKey, testAuthorizedDate) in 
    (select concat(clinicianKey, maxDate) 
      from (select clinicianKey, max(testAuthorizedDate) as maxDate
            from {{ ref('inter_ambryjsondetail__incrmtl') }} 
            group by clinicianKey
            )
    )

    group by clinicianKey, clinicianNPINumberNK
),

final as (

    select * from primaryclinicianref

)

select * from final

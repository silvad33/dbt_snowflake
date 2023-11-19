{{
  config(
    materialized = 'table',
    tags = ['dim'],
    )
}}

with final as (

    select distinct
    patientKey,
    patientAccessionNumberNK,
    patientSex,
    patientEthnicity,
    patientFamilyHistoryTTRFlag,
    recordCreateDateTime as auditCreatedAt,
    recordCreatedByUser as auditCreatedBy        

    from {{ ref('inter_ambryjsondetail__incrmtl') }}

    where patientKey is not null
    /*This next block of logic picks the latest values based on test date, following the latest values from the source to eliminate potential duplication issues. */
    and concat(patientKey, testAuthorizedDate) in 
    (select concat(patientkey, maxDate) 
      from (select patientKey, max(testAuthorizedDate) as maxDate
            from {{ ref('inter_ambryjsondetail__incrmtl') }} 
            group by patientKey
            )
    )

    )

select * from final

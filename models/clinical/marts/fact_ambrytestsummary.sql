{{
  config(
    materialized = 'table',
    tags = ['fact'],
    )
}}

with final as (

    select distinct
    ambryTestKey,
    testingOrganizationKey,
    testingOrganizationAddressKey,
    clinicianKey,
    patientKey,
    patientAccessionNumberNK,
    testAuthorizedDateKey,
    testAuthorizedDate,
    testName,
    testTurnAroundTimeDays,
    ttrResult,
    ttrZygosity,
    ttrClassification,
    ttrNucleotideChange,
    ttrProteinChange,
    testStatus,
    patientAgeAtTest,
    patientDoctorsSeenCount,
    patientPreTestCouncelingFlag,
    patientPostTestCouncelingFlag,
    patientBackpackOptOutFlag,
    patientSymptomArray,
    otherGeneMutationArray,
    recordCreateDateTime as auditCreatedAt,
    recordCreatedByUser as auditCreatedBy
       

    from {{ ref('inter_ambryjsondetail__incrmtl') }}

)

select * from final

{{
  config(
    materialized = 'table',
    tags = ['fact'],
    )
}}

with final as (

    select
    ambryTestDetailKey,
    ambryTestKey,
    testAuthorizedDateKey,
    testingOrganizationKey,
    testingOrganizationAddressKey,
    clinicianKey,
    patientKey,
    symptomKey,
    otherGeneMutationKey,
    patientSymptomKey,
    patientOtherGeneMutationKey,
    patientAccessionNumberNK,
    clinicianSpecialty,
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

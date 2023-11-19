{{
    config(
        materialized = 'view',
        )
}}

with source_ref as (

    select * from {{ source('ambry_json', 'LND_AZURE_PIPE_INGEST_AMBRY_JSON') }}

    where EAUD_LANDING_TIME = (SELECT MAX(EAUD_LANDING_TIME) FROM {{ source('ambry_json', 'LND_AZURE_PIPE_INGEST_AMBRY_JSON') }})

),

renamedSymptoms as (

    select
        {{ dbt_utils.surrogate_key(['record_content:Accession::varchar', 'record_content:AuthorizedDate::date', 'record_content:TestName::varchar', 'record_content:Organization::varchar', 'record_content:Clinician::varchar']) }} as ambryTestKey,
        {{ dbt_utils.surrogate_key(['record_content:Organization::varchar']) }} as testingOrganizationKey,
        record_content:Organization::varchar as testingOrganizationFullName,
        SUBSTR(record_content:Organization::varchar, 1, LENGTH(record_content:Organization::varchar)-7) as testingOrganizationName,
        SUBSTR(RIGHT(record_content:Organization::varchar,7),2,5) as testingOrganizationNumberNK,
        {{ dbt_utils.surrogate_key(['record_content:Address::varchar', 'record_content:City::varchar', 'record_content:State::varchar', 'record_content:ZipCode::varchar', 'record_content:Country::varchar']) }} as testingOrganizationAddressKey,
        record_content:Address::varchar as testingOrganizationStreetAddress,
        record_content:City::varchar as testingOrganizationCity,
        record_content:State::varchar as testingOrganizationState,
        record_content:ZipCode::varchar as testingOrganizationZipCode,
        record_content:Country::varchar as testingOrganizationCountry,
        CASE 
        WHEN record_content:NPI::varchar IS NOT NULL THEN {{ dbt_utils.surrogate_key(['record_content:NPI::varchar']) }}
        ELSE {{ dbt_utils.surrogate_key(['record_content:Clinician::varchar', 'record_content:Physicianemail::varchar', 'record_content:ClinicianPhone::varchar']) }}
        END AS clinicianKey,
        record_content:Clinician::varchar as clinicianName,
        record_content:Physicianemail::varchar as clinicianEmail,
        record_content:ClinicianPhone::varchar as clinicianPhone,
        record_content:NPI::varchar as clinicianNPINumberNK,
        {{ dbt_utils.surrogate_key(['record_content:Specialty::varchar']) }} as specialtyKey,
        record_content:Specialty::varchar as clinicianSpecialty,
        /* --Leaving out Additional Clinician fields per discussion on 12/1--
        {{ dbt_utils.surrogate_key(['record_content:AdditionalContactPhysician::varchar']) }} as additionalClinicianKey,
        record_content:AdditionalContactPhysician::varchar as additionalClinicianName,
        record_content:EmailAddlContact::varchar as additionalClinicianEmail,
        */
        {{ dbt_utils.surrogate_key(['record_content:AuthorizedDate::date']) }} as testAuthorizedDateKey,
        record_content:AuthorizedDate::date as testAuthorizedDate,
        record_content:TestName::varchar as testName,
        record_content:TATDays::integer as testTurnAroundTimeDays,
        record_content:ResultClassificationforTTR::varchar as ttrResult,
        record_content:TTRZygosity::varchar as ttrZygosity,
        record_content:TTRClassification::varchar as ttrClassification,
        record_content:TTRNucleotideChange::varchar as ttrNucleotideChange,
        record_content:TTRProteinChange::varchar as ttrProteinChange,
        record_content:Status::varchar as testStatus,
        {{ dbt_utils.surrogate_key(['record_content:Accession::varchar']) }} as patientKey,
        record_content:Accession::varchar as patientAccessionNumberNK,
        record_content:Sex::varchar as patientSex,
        record_content:Age::integer as patientAgeAtTest,
        record_content:Ethnicity::varchar as patientEthnicity,
        record_content:FamilyHistoryofTTR::varchar as patientFamilyHistoryTTRFlag,
        record_content:Pre::varchar as patientPreTestCouncelingFlag,
        record_content:Post::varchar as patientPostTestCouncelingFlag,
        record_content:BackpackOptOut::varchar as patientBackpackOptOutFlag,
        record_content:XofDoctorsSeen::varchar as patientDoctorsSeenCount,
        EAUD_LANDING_TIME as recordCreateDateTime,
        CURRENT_USER() as recordCreatedByUser,
        {{ dbt_utils.surrogate_key(['s.value:symptom::varchar']) }} as symptomKey,
        s.value:symptom::varchar as patientSymptom,
        s.value:age::varchar as patientSymptomOnsetAge,
        null as otherGeneMutationKey,
        null as otherGeneMutationClassification,
        null as otherGeneMutationGene,
        record_content:symptom::variant as patientSymptomArray,
        record_content:mutations::variant as otherGeneMutationArray

    from source_ref
    , lateral flatten(input=> record_content:symptom, outer => true) s

),

renamedMutations as (

    select
        {{ dbt_utils.surrogate_key(['record_content:Accession::varchar', 'record_content:AuthorizedDate::date', 'record_content:TestName::varchar', 'record_content:Organization::varchar', 'record_content:Clinician::varchar']) }} as ambryTestKey,
        {{ dbt_utils.surrogate_key(['record_content:Organization::varchar']) }} as testingOrganizationKey,
        record_content:Organization::varchar as testingOrganizationFullName,
        SUBSTR(record_content:Organization::varchar, 1, LENGTH(record_content:Organization::varchar)-7) as testingOrganizationName,
        SUBSTR(RIGHT(record_content:Organization::varchar,7),2,5) as testingOrganizationNumberNK,
        {{ dbt_utils.surrogate_key(['record_content:Address::varchar', 'record_content:City::varchar', 'record_content:State::varchar', 'record_content:ZipCode::varchar', 'record_content:Country::varchar']) }} as testingOrganizationAddressKey,
        record_content:Address::varchar as testingOrganizationStreetAddress,
        record_content:City::varchar as testingOrganizationCity,
        record_content:State::varchar as testingOrganizationState,
        record_content:ZipCode::varchar as testingOrganizationZipCode,
        record_content:Country::varchar as testingOrganizationCountry,
        CASE 
        WHEN record_content:NPI::varchar IS NOT NULL THEN {{ dbt_utils.surrogate_key(['record_content:NPI::varchar']) }}
        ELSE {{ dbt_utils.surrogate_key(['record_content:Clinician::varchar', 'record_content:Physicianemail::varchar', 'record_content:ClinicianPhone::varchar']) }}
        END AS clinicianKey,
        record_content:Clinician::varchar as clinicianName,
        record_content:Physicianemail::varchar as clinicianEmail,
        record_content:ClinicianPhone::varchar as clinicianPhone,
        record_content:NPI::varchar as clinicianNPINumberNK,
        {{ dbt_utils.surrogate_key(['record_content:Specialty::varchar']) }} as specialtyKey,
        record_content:Specialty::varchar as clinicianSpecialty,
        /* --Leaving out Additional Clinician fields per discussion on 12/1--
        {{ dbt_utils.surrogate_key(['record_content:AdditionalContactPhysician::varchar']) }} as additionalClinicianKey,
        record_content:AdditionalContactPhysician::varchar as additionalClinicianName,
        record_content:EmailAddlContact::varchar as additionalClinicianEmail,
        */
        {{ dbt_utils.surrogate_key(['record_content:AuthorizedDate::date']) }} as testAuthorizedDateKey,
        record_content:AuthorizedDate::date as testAuthorizedDate,
        record_content:TestName::varchar as testName,
        record_content:TATDays::integer as testTurnAroundTimeDays,
        record_content:ResultClassificationforTTR::varchar as ttrResult,
        record_content:TTRZygosity::varchar as ttrZygosity,
        record_content:TTRClassification::varchar as ttrClassification,
        record_content:TTRNucleotideChange::varchar as ttrNucleotideChange,
        record_content:TTRProteinChange::varchar as ttrProteinChange,
        record_content:Status::varchar as testStatus,
        {{ dbt_utils.surrogate_key(['record_content:Accession::varchar']) }} as patientKey,
        record_content:Accession::varchar as patientAccessionNumberNK,
        record_content:Sex::varchar as patientSex,
        record_content:Age::integer as patientAgeAtTest,
        record_content:Ethnicity::varchar as patientEthnicity,
        record_content:FamilyHistoryofTTR::varchar as patientFamilyHistoryTTRFlag,
        record_content:Pre::varchar as patientPreTestCouncelingFlag,
        record_content:Post::varchar as patientPostTestCouncelingFlag,
        record_content:BackpackOptOut::varchar as patientBackpackOptOutFlag,
        record_content:XofDoctorsSeen::varchar as patientDoctorsSeenCount,
        EAUD_LANDING_TIME as recordCreateDateTime,
        CURRENT_USER() as recordCreatedByUser,
        null as symptomKey,
        null as patientSymptom,
        null as patientSymptomOnsetAge,
        {{ dbt_utils.surrogate_key(['m.value:gene::varchar','m.value:mutationClassification::varchar']) }} as otherGeneMutationKey,
        m.value:mutationClassification::varchar as otherGeneMutationClassification,
        m.value:gene::varchar as otherGeneMutationGene,
        record_content:symptom::variant as patientSymptomArray,
        record_content:mutations::variant as otherGeneMutationArray

    from source_ref
    , lateral flatten(input=> record_content:mutations, outer => true) m

),

renamed as (

    select * from renamedSymptoms
    union all
    select * from renamedMutations

)

select 
distinct
{{ dbt_utils.surrogate_key(['ambryTestKey', 'patientKey', 'symptomKey', 'otherGeneMutationKey']) }} as ambryTestDetailKey,
CASE 
WHEN symptomKey IS NOT NULL THEN {{ dbt_utils.surrogate_key(['patientKey', 'symptomKey']) }} 
ELSE NULL END as patientSymptomKey,
CASE 
WHEN otherGeneMutationKey IS NOT NULL THEN {{ dbt_utils.surrogate_key(['patientKey', 'otherGeneMutationKey']) }} 
ELSE NULL END as patientOtherGeneMutationKey,
/*{{ dbt_utils.surrogate_key(['clinicianKey', 'specialtyKey']) }} as clinicianSpecialtyKey,*/
*

from renamed


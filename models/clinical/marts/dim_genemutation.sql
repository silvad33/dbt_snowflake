{{
  config(
    materialized = 'table',
    tags = ['dim'],
    )
}}

with ref as (

    select distinct
    otherGeneMutationKey as geneMutationKey,
    otherGeneMutationGene as gene,
    otherGeneMutationClassification as mutationClassification,
    recordCreateDateTime as auditCreatedAt,
    recordCreatedByUser as auditCreatedBy
    
    from {{ ref('inter_ambryjsondetail__incrmtl') }}

    where geneMutationKey is not null

),

final as (

    select * from ref

)

select * from final

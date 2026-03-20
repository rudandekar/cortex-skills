{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ood_org_sob_company', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_EL_OOD_ORG_SOB_COMPANY',
        'target_table': 'EL_OOD_ORG_SOB_COMPANY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.663694+00:00'
    }
) }}

WITH 

source_st_ood_org_sob_company AS (
    SELECT
        organization_id,
        organization_name,
        company_code,
        set_of_books_id,
        set_of_books_name,
        create_datetime,
        creation_date,
        last_update_date,
        action_code
    FROM {{ source('raw', 'st_ood_org_sob_company') }}
),

final AS (
    SELECT
        organization_id,
        organization_name,
        company_code,
        set_of_books_id,
        set_of_books_name,
        create_datetime,
        creation_date,
        last_update_date,
        identifier
    FROM source_st_ood_org_sob_company
)

SELECT * FROM final
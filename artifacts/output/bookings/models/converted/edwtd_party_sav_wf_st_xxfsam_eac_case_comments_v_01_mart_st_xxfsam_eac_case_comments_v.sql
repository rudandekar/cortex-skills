{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_eac_case_comments_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_EAC_CASE_COMMENTS_V',
        'target_table': 'ST_XXFSAM_EAC_CASE_COMMENTS_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.971268+00:00'
    }
) }}

WITH 

source_ff_xxfsam_eac_case_comments_v AS (
    SELECT
        comment_id,
        case_id,
        text,
        comment_last_update_date,
        commented_by,
        organization_name,
        organization_id
    FROM {{ source('raw', 'ff_xxfsam_eac_case_comments_v') }}
),

final AS (
    SELECT
        comment_id,
        case_id,
        text,
        comment_last_update_date,
        commented_by,
        organization_name,
        organization_id
    FROM source_ff_xxfsam_eac_case_comments_v
)

SELECT * FROM final
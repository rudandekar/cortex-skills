{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_pss_opportunity_assessment', 'batch', 'edwtd_mkt_cic'],
    meta={
        'source_workflow': 'wf_m_SM_PSS_OPPORTUNITY_ASSESSMENT',
        'target_table': 'SM_PSS_OPPORTUNITY_ASSESSMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.835711+00:00'
    }
) }}

WITH 

source_sm_pss_opportunity_assessment AS (
    SELECT
        pss_opportunity_assessment_key,
        sfdc_workspace_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_pss_opportunity_assessment') }}
),

final AS (
    SELECT
        pss_opportunity_assessment_key,
        sfdc_workspace_id,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_pss_opportunity_assessment
)

SELECT * FROM final
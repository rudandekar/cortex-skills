{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_rae_cogs_percent_rao', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_SM_RAE_COGS_PERCENT_RAO',
        'target_table': 'SM_RAE_COGS_PERCENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.363136+00:00'
    }
) }}

WITH 

source_sm_rae_cogs_percent AS (
    SELECT
        cogs_percent_key,
        sk_cogs_percent_id,
        ss_cd,
        schedule_line_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_rae_cogs_percent') }}
),

final AS (
    SELECT
        cogs_percent_key,
        sk_cogs_percent_id,
        ss_cd,
        schedule_line_id,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_rae_cogs_percent
)

SELECT * FROM final
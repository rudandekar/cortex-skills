{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_gct_reasons', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_GCT_REASONS',
        'target_table': 'ST_OTM_GCT_REASONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.085281+00:00'
    }
) }}

WITH 

source_ff_otm_gct_reasons AS (
    SELECT
        reason_code,
        claim_reason_name,
        reason_type,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'ff_otm_gct_reasons') }}
),

final AS (
    SELECT
        reason_code,
        claim_reason_name,
        reason_type,
        batch_id,
        create_datetime
    FROM source_ff_otm_gct_reasons
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_gct_pos_lines', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_GCT_POS_LINES',
        'target_table': 'ST_OTM_GCT_POS_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.437195+00:00'
    }
) }}

WITH 

source_ff_otm_gct_pos_lines AS (
    SELECT
        pos_line_id,
        claim_id,
        pos_transaction_id,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'ff_otm_gct_pos_lines') }}
),

final AS (
    SELECT
        pos_line_id,
        claim_id,
        pos_transaction_id,
        batch_id,
        create_datetime
    FROM source_ff_otm_gct_pos_lines
)

SELECT * FROM final
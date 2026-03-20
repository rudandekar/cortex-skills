{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cma_trx_prts_ext_pos', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CMA_TRX_PRTS_EXT_POS',
        'target_table': 'ST_CMA_TRX_PRTS_EXT_POS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.632423+00:00'
    }
) }}

WITH 

source_ff_cma_trx_prts_ext_pos AS (
    SELECT
        trx_party_ext_id,
        pos_trans_id,
        cr_global_ultimate,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'ff_cma_trx_prts_ext_pos') }}
),

final AS (
    SELECT
        trx_party_ext_id,
        pos_trans_id,
        cr_global_ultimate,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        last_update_date,
        batch_id
    FROM source_ff_cma_trx_prts_ext_pos
)

SELECT * FROM final
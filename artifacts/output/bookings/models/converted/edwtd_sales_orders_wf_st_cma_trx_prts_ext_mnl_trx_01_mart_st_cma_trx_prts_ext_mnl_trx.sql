{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cma_trx_prts_ext_mnl_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CMA_TRX_PRTS_EXT_MNL_TRX',
        'target_table': 'ST_CMA_TRX_PRTS_EXT_MNL_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.057481+00:00'
    }
) }}

WITH 

source_ff_cma_trx_prts_ext_mnl_trx AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        cr_global_ultimate,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        trx_orig_code,
        trx_source_type,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'ff_cma_trx_prts_ext_mnl_trx') }}
),

final AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        cr_global_ultimate,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        trx_orig_code,
        trx_source_type,
        last_update_date,
        batch_id
    FROM source_ff_cma_trx_prts_ext_mnl_trx
)

SELECT * FROM final
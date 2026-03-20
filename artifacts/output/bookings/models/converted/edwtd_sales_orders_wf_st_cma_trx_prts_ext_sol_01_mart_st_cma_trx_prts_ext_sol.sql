{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cma_trx_prts_ext_sol', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CMA_TRX_PRTS_EXT_SOL',
        'target_table': 'ST_CMA_TRX_PRTS_EXT_SOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.603324+00:00'
    }
) }}

WITH 

source_ff_cma_trx_prts_ext_sol AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        cr_global_ultimate,
        global_name,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'ff_cma_trx_prts_ext_sol') }}
),

transformed_exptrans AS (
    SELECT
    trx_party_ext_id,
    order_line_id,
    cr_global_ultimate,
    global_name,
    theatre,
    start_date,
    expiration_date,
    enrichment_code,
    last_update_date,
    batch_id
    FROM source_ff_cma_trx_prts_ext_sol
),

final AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        cr_global_ultimate,
        global_name,
        theatre,
        start_date,
        expiration_date,
        enrichment_code,
        last_update_date,
        batch_id
    FROM transformed_exptrans
)

SELECT * FROM final
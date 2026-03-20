{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_phx_trx_sol', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_PHX_TRX_SOL',
        'target_table': 'ST_OTM_PHX_TRX_SOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.406791+00:00'
    }
) }}

WITH 

source_ff_otm_phx_trx_sol AS (
    SELECT
        trx_id,
        order_line_id,
        global_name,
        party_ext_id,
        enrichment_code,
        enrichment_party_type,
        pgtmv_party_be_id,
        class_code,
        user_name,
        creation_date,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'ff_otm_phx_trx_sol') }}
),

final AS (
    SELECT
        trx_id,
        order_line_id,
        global_name,
        party_ext_id,
        enrichment_code,
        enrichment_party_type,
        pgtmv_party_be_id,
        class_code,
        user_name,
        creation_date,
        last_update_date,
        batch_id
    FROM source_ff_otm_phx_trx_sol
)

SELECT * FROM final
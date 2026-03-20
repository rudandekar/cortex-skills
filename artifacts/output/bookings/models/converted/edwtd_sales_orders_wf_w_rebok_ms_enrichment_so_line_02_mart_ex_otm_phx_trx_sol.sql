{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rebok_ms_enrichment_so_line', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_REBOK_MS_ENRICHMENT_SO_LINE',
        'target_table': 'EX_OTM_PHX_TRX_SOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.626171+00:00'
    }
) }}

WITH 

source_ex_otm_phx_trx_sol AS (
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
        batch_id,
        exception_type
    FROM {{ source('raw', 'ex_otm_phx_trx_sol') }}
),

source_st_otm_phx_trx_sol AS (
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
    FROM {{ source('raw', 'st_otm_phx_trx_sol') }}
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
        batch_id,
        exception_type
    FROM source_st_otm_phx_trx_sol
)

SELECT * FROM final
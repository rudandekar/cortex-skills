{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rebok_ms_enrichment_mnl_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_REBOK_MS_ENRICHMENT_MNL_TRX',
        'target_table': 'EX_OTM_PHX_TRX_MNL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.017433+00:00'
    }
) }}

WITH 

source_ex_otm_phx_trx_mnl AS (
    SELECT
        trx_id,
        order_line_id,
        trx_orig_code,
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
    FROM {{ source('raw', 'ex_otm_phx_trx_mnl') }}
),

source_st_otm_phx_trx_mnl AS (
    SELECT
        trx_id,
        order_line_id,
        trx_orig_code,
        party_ext_id,
        enrichment_code,
        enrichment_party_type,
        pgtmv_party_be_id,
        class_code,
        user_name,
        creation_date,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'st_otm_phx_trx_mnl') }}
),

final AS (
    SELECT
        trx_id,
        order_line_id,
        trx_orig_code,
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
    FROM source_st_otm_phx_trx_mnl
)

SELECT * FROM final
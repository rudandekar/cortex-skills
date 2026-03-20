{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rebok_ms_enrchmnt_nappld_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_REBOK_MS_ENRCHMNT_NAPPLD_TRX',
        'target_table': 'EX_OTM_PHX_TRX_NAPLD_REVLN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.999130+00:00'
    }
) }}

WITH 

source_ex_otm_phx_trx_napld_revln AS (
    SELECT
        trx_id,
        cust_trx_line_id,
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
    FROM {{ source('raw', 'ex_otm_phx_trx_napld_revln') }}
),

source_st_otm_phx_trx_napld_revln AS (
    SELECT
        trx_id,
        cust_trx_line_id,
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
    FROM {{ source('raw', 'st_otm_phx_trx_napld_revln') }}
),

final AS (
    SELECT
        trx_id,
        cust_trx_line_id,
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
    FROM source_st_otm_phx_trx_napld_revln
)

SELECT * FROM final
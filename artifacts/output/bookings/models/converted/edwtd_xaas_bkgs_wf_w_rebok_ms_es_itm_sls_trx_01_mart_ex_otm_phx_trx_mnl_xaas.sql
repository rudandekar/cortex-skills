{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rebok_ms_es_itm_sls_trx', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_W_REBOK_MS_ES_ITM_SLS_TRX',
        'target_table': 'EX_OTM_PHX_TRX_MNL_XAAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.812808+00:00'
    }
) }}

WITH 

source_ex_otm_phx_trx_mnl_xaas AS (
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
    FROM {{ source('raw', 'ex_otm_phx_trx_mnl_xaas') }}
),

source_st_otm_phx_trx_mnl_xaas AS (
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
    FROM {{ source('raw', 'st_otm_phx_trx_mnl_xaas') }}
),

source_w_rebok_ms_es_itm_sls_trx AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        ms_partner_pty_key,
        modified_by_user_id,
        ms_identification_method_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rebok_ms_es_itm_sls_trx') }}
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
    FROM source_w_rebok_ms_es_itm_sls_trx
)

SELECT * FROM final
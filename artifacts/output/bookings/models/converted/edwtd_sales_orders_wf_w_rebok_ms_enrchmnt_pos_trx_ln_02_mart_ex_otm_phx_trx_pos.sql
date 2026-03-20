{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rebok_ms_enrchmnt_pos_trx_ln', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_REBOK_MS_ENRCHMNT_POS_TRX_LN',
        'target_table': 'EX_OTM_PHX_TRX_POS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.420695+00:00'
    }
) }}

WITH 

source_ex_otm_phx_trx_pos AS (
    SELECT
        trx_id,
        pos_trans_id,
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
    FROM {{ source('raw', 'ex_otm_phx_trx_pos') }}
),

source_st_otm_phx_trx_pos AS (
    SELECT
        trx_id,
        pos_trans_id,
        party_ext_id,
        enrichment_code,
        enrichment_party_type,
        pgtmv_party_be_id,
        class_code,
        user_name,
        creation_date,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'st_otm_phx_trx_pos') }}
),

source_st_otm_phx_trx_pos AS (
    SELECT
        trx_id,
        pos_trans_id,
        party_ext_id,
        enrichment_code,
        enrichment_party_type,
        pgtmv_party_be_id,
        class_code,
        user_name,
        creation_date,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'st_otm_phx_trx_pos') }}
),

final AS (
    SELECT
        trx_id,
        pos_trans_id,
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
    FROM source_st_otm_phx_trx_pos
)

SELECT * FROM final
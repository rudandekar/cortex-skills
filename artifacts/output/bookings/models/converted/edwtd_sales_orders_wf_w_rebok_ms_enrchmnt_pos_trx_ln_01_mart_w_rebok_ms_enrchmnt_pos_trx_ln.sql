{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rebok_ms_enrchmnt_pos_trx_ln', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_REBOK_MS_ENRCHMNT_POS_TRX_LN',
        'target_table': 'W_REBOK_MS_ENRCHMNT_POS_TRX_LN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.105664+00:00'
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
        bk_pos_transaction_id_int,
        ms_partner_party_key,
        ms_identification_method_int,
        modified_by_user_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_otm_phx_trx_pos
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_subui_ar_transactions', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_ST_SUBUI_AR_TRANSACTIONS',
        'target_table': 'ST_SUBUI_AR_TRANSACTIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.284405+00:00'
    }
) }}

WITH 

source_st_si_subui_ar_transactions AS (
    SELECT
        sk_subui_ref_transaction_id,
        transaction_type,
        transaction_sub_type,
        subscription_reference_id,
        subscription_code,
        tier1_reason_code,
        tier1_reason_description,
        comments,
        tier2_reason_code,
        tier2_reason_description,
        web_order_id,
        edw_create_dtm,
        additional_info
    FROM {{ source('raw', 'st_si_subui_ar_transactions') }}
),

source_sm_subscr_auto_rnwl_chg AS (
    SELECT
        subscr_auto_rnwl_change_key,
        bk_web_order_id,
        sk_subui_trx_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_subscr_auto_rnwl_chg') }}
),

final AS (
    SELECT
        sk_subui_ref_transaction_id,
        transaction_type,
        transaction_sub_type,
        subscription_reference_id,
        subscription_code,
        tier1_reason_code,
        tier1_reason_description,
        comments,
        tier2_reason_code,
        tier2_reason_description,
        web_order_id,
        edw_create_dtm,
        additional_info
    FROM source_sm_subscr_auto_rnwl_chg
)

SELECT * FROM final
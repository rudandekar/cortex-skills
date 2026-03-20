{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_subscr_auto_rnwl_chg', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_SUBSCR_AUTO_RNWL_CHG',
        'target_table': 'N_SUBSCR_AUTO_RNWL_CHG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.083950+00:00'
    }
) }}

WITH 

source_w_subscr_auto_rnwl_chg AS (
    SELECT
        subscr_auto_rnwl_change_key,
        bk_web_order_id,
        bk_subscription_ref_id,
        bk_trx_type_name,
        bk_trx_sub_type_name,
        sk_subui_trx_id,
        subscription_cd,
        tier_1_reason_descr,
        tier_2_reason_descr,
        comments_txt,
        tier_1_reason_cd,
        tier_2_reason_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        auto_rnwl_addnl_info_txt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_subscr_auto_rnwl_chg') }}
),

final AS (
    SELECT
        subscr_auto_rnwl_change_key,
        bk_web_order_id,
        bk_subscription_ref_id,
        bk_trx_type_name,
        bk_trx_sub_type_name,
        sk_subui_trx_id,
        subscription_cd,
        tier_1_reason_descr,
        tier_2_reason_descr,
        comments_txt,
        tier_1_reason_cd,
        tier_2_reason_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        auto_rnwl_addnl_info_txt
    FROM source_w_subscr_auto_rnwl_chg
)

SELECT * FROM final
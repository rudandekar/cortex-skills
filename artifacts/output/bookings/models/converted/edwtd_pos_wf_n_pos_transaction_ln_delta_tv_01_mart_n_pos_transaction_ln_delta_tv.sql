{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_transaction_ln_delta_tv', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_POS_TRANSACTION_LN_DELTA_TV',
        'target_table': 'N_POS_TRANSACTION_LN_DELTA_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.540503+00:00'
    }
) }}

WITH 

source_n_pos_transaction_ln_delta_tv AS (
    SELECT
        bk_pos_trx_id_int,
        base_list_unit_prdt_price_amt,
        validated_net_unt_prc_usd_amt,
        dstrbtr_rptd_cst_unt_prc_amt,
        valuation_price_usd_amt,
        pos_trx_ln_prdt_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        ru_srvc_cntrct_drtn_mnths_cnt,
        cisco_internal_pos_flg,
        is_service_contract_pos_flg,
        glob_unit_list_price_usd_amt,
        glob_unit_list_price_local_amt,
        transaction_ai_flg,
        ai_intent_type_cd,
        trx_ai_product_class_name,
        ai_strategic_factor_pct
    FROM {{ source('raw', 'n_pos_transaction_ln_delta_tv') }}
),

final AS (
    SELECT
        bk_pos_trx_id_int,
        base_list_unit_prdt_price_amt,
        validated_net_unt_prc_usd_amt,
        dstrbtr_rptd_cst_unt_prc_amt,
        valuation_price_usd_amt,
        pos_trx_ln_prdt_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        ru_srvc_cntrct_drtn_mnths_cnt,
        cisco_internal_pos_flg,
        is_service_contract_pos_flg,
        glob_unit_list_price_usd_amt,
        glob_unit_list_price_local_amt,
        transaction_ai_flg,
        ai_intent_type_cd,
        trx_ai_product_class_name,
        ai_strategic_factor_pct
    FROM source_n_pos_transaction_ln_delta_tv
)

SELECT * FROM final
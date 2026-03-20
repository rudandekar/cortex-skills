{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pos_sca_incr_pos', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_POS_SCA_INCR_POS',
        'target_table': 'WI_POS_SCA_INCR_POS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.406837+00:00'
    }
) }}

WITH 

source_wi_pos_sca_incr_pos AS (
    SELECT
        pos_scaac_key,
        pd_bk_pos_transaction_id_int,
        base_list_unit_prod_price_amt,
        disti_rptd_cost_unit_price_amt,
        vldtd_net_unit_price_usd_amt,
        valuation_price_usd_amount,
        pd_sales_territory_key,
        pd_sales_rep_num,
        distributor_offset_flg,
        pd_sales_commission_pct,
        sc_id_int,
        source_deleted_flg,
        last_updated_dtm,
        dv_last_updated_dt,
        edw_update_dtm,
        start_tv_dtm,
        end_tv_dtm,
        distributor_bucket_flg,
        pos_trx_ln_prdt_qty,
        sk_trx_sc_id_int,
        dsv_or_pos_type_cd,
        pos_return_role,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        ru_srvc_cntrct_drtn_mnths_cnt,
        glbl_prc_lst_unt_prc_usd_amt,
        glbo_prc_lst_unt_prc_lcl_amt,
        transaction_ai_flg,
        ai_intent_type_cd,
        trx_ai_product_class_name,
        ai_strategic_factor_pct
    FROM {{ source('raw', 'wi_pos_sca_incr_pos') }}
),

final AS (
    SELECT
        pos_scaac_key,
        pd_bk_pos_transaction_id_int,
        base_list_unit_prod_price_amt,
        disti_rptd_cost_unit_price_amt,
        vldtd_net_unit_price_usd_amt,
        valuation_price_usd_amount,
        pd_sales_territory_key,
        pd_sales_rep_num,
        distributor_offset_flg,
        pd_sales_commission_pct,
        sc_id_int,
        source_deleted_flg,
        last_updated_dtm,
        dv_last_updated_dt,
        edw_update_dtm,
        start_tv_dtm,
        end_tv_dtm,
        distributor_bucket_flg,
        pos_trx_ln_prdt_qty,
        sk_trx_sc_id_int,
        dsv_or_pos_type_cd,
        pos_return_role,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        ru_srvc_cntrct_drtn_mnths_cnt,
        glbl_prc_lst_unt_prc_usd_amt,
        glbo_prc_lst_unt_prc_lcl_amt,
        transaction_ai_flg,
        ai_intent_type_cd,
        trx_ai_product_class_name,
        ai_strategic_factor_pct
    FROM source_wi_pos_sca_incr_pos
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_distributor_promotion', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTRIBUTOR_PROMOTION',
        'target_table': 'N_DISTRIBUTOR_PROMOTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.381785+00:00'
    }
) }}

WITH 

source_w_distributor_promotion_old AS (
    SELECT
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        promotion_bundle_flg,
        promotion_category_type_cd,
        source_created_by_dtm,
        source_last_updated_dtm,
        created_by_as_reported_user,
        updated_by_as_reported_user,
        promotion_price_list_type_cd,
        maximum_allowable_deal_amt,
        promotion_effective_start_dt,
        reported_promotion_theatre_cd,
        promotion_effective_end_dt,
        max_days_allwd_prmtn_claim_cnt,
        promotion_source_name,
        promotion_short_descr,
        promotion_name,
        standard_bulletin_num,
        sk_promo_id_int,
        promotion_initiator_name,
        prmtn_aprvd_csco_wrkr_prty_key,
        src_rptd_promotion_apprvr_name,
        source_deleted_flg,
        transactional_currency_cd,
        bk_deal_id,
        wholesale_price_list_name,
        global_price_list_name,
        prod_fmly_prmtn_prc_list_name,
        chnl_acct_mgr_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_distributor_promotion_old') }}
),

final AS (
    SELECT
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        promotion_bundle_flg,
        promotion_category_type_cd,
        source_created_by_dtm,
        source_last_updated_dtm,
        created_by_as_reported_user,
        updated_by_as_reported_user,
        promotion_price_list_type_cd,
        maximum_allowable_deal_amt,
        promotion_effective_start_dt,
        reported_promotion_theatre_cd,
        promotion_effective_end_dt,
        max_days_allwd_prmtn_claim_cnt,
        promotion_source_name,
        promotion_short_descr,
        promotion_name,
        standard_bulletin_num,
        sk_promo_id_int,
        promotion_initiator_name,
        prmtn_aprvd_csco_wrkr_prty_key,
        src_rptd_promotion_apprvr_name,
        source_deleted_flg,
        transactional_currency_cd,
        bk_deal_id,
        wholesale_price_list_name,
        global_price_list_name,
        prod_fmly_prmtn_prc_list_name,
        chnl_acct_mgr_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        promotion_split_flg,
        market_type_cd,
        channel_type_cd
    FROM source_w_distributor_promotion_old
)

SELECT * FROM final
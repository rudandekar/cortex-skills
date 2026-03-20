{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_ww_bkg_deal_promo_master', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_WW_BKG_DEAL_PROMO_MASTER',
        'target_table': 'MT_WW_BKG_DEAL_PROMO_MASTER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.527301+00:00'
    }
) }}

WITH 

source_mt_ww_prc_claims_quote AS (
    SELECT
        ru_bk_pos_transaction_id_int,
        bk_deal_id,
        product_key,
        dv_promotion_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        adjustment_type_cd,
        dv_source_cd,
        dv_program_cd,
        dv_deal_promotion_cd,
        dv_deal_program_cd,
        dv_seq_int
    FROM {{ source('raw', 'mt_ww_prc_claims_quote') }}
),

final AS (
    SELECT
        ru_bk_pos_transaction_id_int,
        product_key,
        bk_deal_id,
        adjustment_type_cd,
        dv_line_ft_flg,
        dv_line_ltm_flg,
        dv_line_volume_pp_flg,
        dv_line_cisco_start_flg,
        dv_line_oth_pp_flg,
        dv_line_volume_br_flg,
        dv_line_xip_flg,
        dv_line_k12_flg,
        dv_line_oth_br_flg,
        dv_deal_volume_br_flg,
        dv_deal_xip_flg,
        dv_deal_k12_flg,
        dv_deal_oth_br_flg,
        dv_contract_flg,
        dv_rtm_cd,
        dv_volume_flg,
        dv_promotion_1_cd,
        dv_promotion_2_cd,
        dv_promotion_3_cd,
        dv_promotion_4_cd,
        dv_promotion_5_cd,
        dv_promotion_6_cd,
        dv_promotion_7_cd,
        dv_promotion_8_cd,
        dv_deal_promotion_1_cd,
        dv_deal_promotion_2_cd,
        dv_deal_promotion_3_cd,
        dv_deal_promotion_4_cd,
        dv_deal_promotion_5_cd,
        dv_promotion_cd,
        dv_deal_promotion_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_deal_oip_flg,
        dv_oipx_flg,
        dv_program_cd
    FROM source_mt_ww_prc_claims_quote
)

SELECT * FROM final
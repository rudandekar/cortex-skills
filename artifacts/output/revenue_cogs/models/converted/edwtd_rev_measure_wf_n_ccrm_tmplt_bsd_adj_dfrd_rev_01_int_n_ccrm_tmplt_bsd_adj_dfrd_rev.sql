{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_n_ccrm_tmplt_bsd_adj_dfrd_rev', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_TMPLT_BSD_ADJ_DFRD_REV',
        'target_table': 'N_CCRM_TMPLT_BSD_ADJ_DFRD_REV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.186995+00:00'
    }
) }}

WITH 

source_n_ccrm_tmplt_bsd_adj_dfrd_rev AS (
    SELECT
        ccrm_tmplt_bsd_adj_dfr_rev_key,
        bk_prcssd_fiscal_cal_cd,
        bk_prcssd_fiscal_year_num_int,
        bk_prcssd_fiscal_mth_num_int,
        bk_deal_id,
        sales_territory_key,
        sales_order_line_key,
        bk_measure_name,
        bk_rev_measurement_type_cd,
        bk_ccrm_profile_id_int,
        bk_agreement_name,
        bk_ccrm_adj_reason_cd,
        bk_ccrm_adj_rsn_vrsn_num_int,
        bk_ccrm_acct_element_cd,
        attributed_flg,
        adjustment_revenue_usd_amt,
        product_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_top_sku_prdt_key,
        bk_offer_attri_cd,
        bk_offer_attri_id_int,
        bk_service_flg,
        active_flg,
        order_src_cd,
        order_sub_src_cd
    FROM {{ source('raw', 'n_ccrm_tmplt_bsd_adj_dfrd_rev') }}
),

final AS (
    SELECT
        ccrm_tmplt_bsd_adj_dfr_rev_key,
        bk_prcssd_fiscal_cal_cd,
        bk_prcssd_fiscal_year_num_int,
        bk_prcssd_fiscal_mth_num_int,
        bk_deal_id,
        sales_territory_key,
        sales_order_line_key,
        bk_measure_name,
        bk_rev_measurement_type_cd,
        bk_ccrm_profile_id_int,
        bk_agreement_name,
        bk_ccrm_adj_reason_cd,
        bk_ccrm_adj_rsn_vrsn_num_int,
        bk_ccrm_acct_element_cd,
        attributed_flg,
        adjustment_revenue_usd_amt,
        product_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_top_sku_prdt_key,
        bk_offer_attri_cd,
        bk_offer_attri_id_int,
        bk_service_flg,
        active_flg,
        order_src_cd,
        order_sub_src_cd
    FROM source_n_ccrm_tmplt_bsd_adj_dfrd_rev
)

SELECT * FROM final
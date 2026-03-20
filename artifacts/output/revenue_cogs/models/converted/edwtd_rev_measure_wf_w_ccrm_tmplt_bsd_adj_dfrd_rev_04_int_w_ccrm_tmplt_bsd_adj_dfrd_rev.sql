{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_w_ccrm_tmplt_bsd_adj_dfrd_rev', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_TMPLT_BSD_ADJ_DFRD_REV',
        'target_table': 'W_CCRM_TMPLT_BSD_ADJ_DFRD_REV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.756479+00:00'
    }
) }}

WITH 

source_wi_ccrm_defrev_tba_services AS (
    SELECT
        fiscal_year_month_int,
        rev_measurement_type_cd,
        profile_id,
        agreement_name,
        reason_code,
        deal_id,
        line_type,
        so_line_id,
        sales_order_line_key,
        sales_territory_key,
        def_amt,
        bk_ccrm_adj_reason_ver_num_int,
        attributed_flg,
        product_key,
        bk_service_flg,
        active_flag,
        order_source,
        order_sub_source
    FROM {{ source('raw', 'wi_ccrm_defrev_tba_services') }}
),

source_wi_ccrm_defrev_tba_prdt AS (
    SELECT
        fiscal_year_month_int,
        rev_measurement_type_cd,
        profile_id,
        agreement_name,
        reason_code,
        deal_id,
        line_type,
        so_line_id,
        sales_order_line_key,
        sales_territory_key,
        def_amt,
        bk_ccrm_adj_reason_ver_num_int,
        attributed_flg,
        product_key,
        bk_service_flg,
        active_flag,
        order_source,
        order_sub_source
    FROM {{ source('raw', 'wi_ccrm_defrev_tba_prdt') }}
),

source_wi_ccrm_defrev_tba_cat_offer AS (
    SELECT
        fiscal_calendar_code,
        fiscal_year_number_int,
        fiscal_month_number_int,
        rev_measurement_type_cd,
        profile_id,
        agreement_name,
        reason_code,
        deal_id,
        line_type,
        so_line_id,
        sales_order_line_key,
        sales_territory_key,
        def_amt,
        bk_ccrm_adj_reason_ver_num_int,
        measure_name,
        attributed_flg,
        product_key,
        dv_top_sku_prdt_key,
        bk_offer_attri_cd,
        bk_offer_attri_id_int,
        bk_service_flg,
        active_flag,
        order_source,
        order_sub_source
    FROM {{ source('raw', 'wi_ccrm_defrev_tba_cat_offer') }}
),

source_w_ccrm_tmplt_bsd_adj_dfrd_rev AS (
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
        order_source,
        order_sub_source,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_tmplt_bsd_adj_dfrd_rev') }}
),

source_sm_ccrm_tmplt_bsd_adj_dfrd_rev AS (
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
        product_key,
        dv_top_sku_prdt_key,
        bk_offer_attri_cd,
        bk_offer_attri_id_int,
        bk_service_flg,
        edw_create_dtm,
        edw_create_user,
        order_source,
        order_sub_source
    FROM {{ source('raw', 'sm_ccrm_tmplt_bsd_adj_dfrd_rev') }}
),

source_wi_ccrm_defrev_tba_cat AS (
    SELECT
        fiscal_calendar_code,
        fiscal_year_number_int,
        fiscal_month_number_int,
        rev_measurement_type_cd,
        profile_id,
        agreement_name,
        reason_code,
        deal_id,
        line_type,
        so_line_id,
        sales_order_line_key,
        sales_territory_key,
        def_amt,
        bk_ccrm_adj_reason_ver_num_int,
        measure_name,
        attributed_flg,
        product_key,
        bk_service_flg,
        active_flag,
        order_source,
        order_sub_source
    FROM {{ source('raw', 'wi_ccrm_defrev_tba_cat') }}
),

source_wi_ccrm_defrev_tba AS (
    SELECT
        fiscal_year_month_int,
        rev_measurement_type_cd,
        profile_id,
        agreement_name,
        reason_code,
        deal_id,
        line_type,
        so_line_id,
        sales_order_line_key,
        sales_territory_key,
        def_amt,
        bk_ccrm_adj_reason_ver_num_int,
        attributed_flg,
        product_key,
        bk_service_flg,
        active_flag,
        order_source,
        order_sub_source
    FROM {{ source('raw', 'wi_ccrm_defrev_tba') }}
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
        order_sub_src_cd,
        action_code,
        dml_type
    FROM source_wi_ccrm_defrev_tba
)

SELECT * FROM final
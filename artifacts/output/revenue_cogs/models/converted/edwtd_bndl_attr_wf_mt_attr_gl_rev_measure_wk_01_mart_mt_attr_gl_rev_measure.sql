{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_attr_gl_rev_measure_wk', 'batch', 'edwtd_bndl_attr'],
    meta={
        'source_workflow': 'wf_m_MT_ATTR_GL_REV_MEASURE_WK',
        'target_table': 'MT_ATTR_GL_REV_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.967672+00:00'
    }
) }}

WITH 

source_wi_gl_curr_qrtr_m1_month AS (
    SELECT
        revenue_measure_key,
        product_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        dv_corporate_revenue_flg,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        pob_type_cd,
        nrs_transition_flg,
        retained_earnings_flg,
        dd_comp_us_orig_list_price,
        dd_comp_us_orig_ext_list_price
    FROM {{ source('raw', 'wi_gl_curr_qrtr_m1_month') }}
),

final AS (
    SELECT
        revenue_measure_key,
        product_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        dv_corporate_revenue_flg,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        attributed_flg,
        bundle_product_key,
        bk_handshake_bundle_type_name,
        dv_attribution_cd,
        pob_type_cd,
        nrs_transition_flg,
        retained_earnings_flg,
        dd_comp_us_orig_list_price,
        dd_comp_us_orig_ext_list_price
    FROM source_wi_gl_curr_qrtr_m1_month
)

SELECT * FROM final
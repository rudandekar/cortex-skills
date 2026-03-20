{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rev_msr_non_bundle_me', 'batch', 'edwtd_bndl_attr'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REV_MSR_NON_BUNDLE_ME',
        'target_table': 'WI_GL_MSR_NONBNDL_QRT_CUR_MNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.248560+00:00'
    }
) }}

WITH 

source_wi_gl_curr_last_qrtr AS (
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
        dd_comp_us_orig_list_price,
        dd_comp_us_orig_ext_list_price
    FROM {{ source('raw', 'wi_gl_curr_last_qrtr') }}
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
        dv_attribution_cd,
        dv_product_key,
        pob_type_cd,
        nrs_transition_flg,
        retained_earnings_flg,
        dd_comp_us_orig_list_price,
        dd_comp_us_orig_ext_list_price
    FROM source_wi_gl_curr_last_qrtr
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_aud_rstd_aggr_gl_rev_pidter_mth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_AUD_RSTD_AGGR_GL_REV_PIDTER_MTH',
        'target_table': 'WI_AUD_RSTD_REV_THRSHLD_VALUES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.958769+00:00'
    }
) }}

WITH 

source_wi_aud_rstd_gl_rev_meas_mth AS (
    SELECT
        audit_table_name,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_prc_amt,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        gl_distrib_functional_amt,
        gl_distrib_transactional_amt,
        dv_comp_func_net_price_amt,
        dv_comp_func_gross_rev_amt,
        dv_comp_func_net_rev_amt,
        dd_extended_net_qty,
        dd_extended_gross_qty,
        dv_extended_qty,
        dv_rev_std_cost_adj_amt,
        dv_rev_adj_amt,
        dv_rev_adj_ext_lst_amt,
        dv_drct_rev_adj_amt,
        dv_drct_rev_st_cst_adj_amt,
        dv_indrct_rev_adj_amt,
        dv_indrct_rv_st_cst_ad_amt,
        dv_std_cost_amt,
        dv_adj_ext_list_amt,
        dv_units_wzfilt_net_qty,
        dv_gross_units_wzfilt_qty,
        dv_net_units_wzfilt_qty,
        dv_mktshr_units_wzfilt_qty,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'wi_aud_rstd_gl_rev_meas_mth') }}
),

source_wi_aud_rstd_rev_thrshld_values AS (
    SELECT
        audit_table_name,
        audit_column_name,
        min_threshold_value,
        max_threshold_value,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'wi_aud_rstd_rev_thrshld_values') }}
),

transformed_exptrans AS (
    SELECT
    audit_table_name,
    audit_column_name,
    amountdiff_flg,
    IFF(AMOUNTDIFF_FLG = 'Y', ABORT('Amounts are not matching')) AS amountdiff_flg_out
    FROM source_wi_aud_rstd_rev_thrshld_values
),

filtered_filtrans AS (
    SELECT *
    FROM transformed_exptrans
    WHERE FALSE
),

final AS (
    SELECT
        audit_table_name,
        audit_column_name,
        min_threshold_value,
        max_threshold_value,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM filtered_filtrans
)

SELECT * FROM final
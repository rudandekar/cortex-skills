{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_rstd_bkgs_gross_margin', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_RSTD_BKGS_GROSS_MARGIN',
        'target_table': 'MT_RSTD_BKGS_GROSS_MARGIN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.297795+00:00'
    }
) }}

WITH 

source_mt_rstd_bkgs_gross_margin AS (
    SELECT
        bookings_measure_key,
        product_key,
        sales_territory_key,
        sales_rep_num,
        bookings_process_dt,
        dv_fiscal_year_mth_number_int,
        bkgs_measure_trans_type_cd,
        corporate_bookings_flg,
        dd_comp_us_net_price_amt,
        dd_comp_us_cost_amt,
        dv_bgm_non_standard_cost_amt,
        dv_bkg_gross_mgn_amt,
        dv_bgm_fx_cost_amt,
        dv_fx_net_price_amt,
        dv_fx_bgm_amt,
        internal_sbe_name,
        internal_sbe_descr,
        internal_be_name,
        internal_be_descr,
        external_sbe_name,
        external_sbe_descr,
        external_be_name,
        external_be_descr,
        dd_comp_us_list_price_amt,
        dd_extended_qty,
        dv_revenue_recognition_flg,
        adjustment_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        acquisition_order_origin_cd,
        acquisition_role
    FROM {{ source('raw', 'mt_rstd_bkgs_gross_margin') }}
),

final AS (
    SELECT
        bookings_measure_key,
        product_key,
        sales_territory_key,
        sales_rep_num,
        bookings_process_dt,
        dv_fiscal_year_mth_number_int,
        bkgs_measure_trans_type_cd,
        corporate_bookings_flg,
        dd_comp_us_net_price_amt,
        dd_comp_us_cost_amt,
        dv_bgm_non_standard_cost_amt,
        dv_bkg_gross_mgn_amt,
        dv_bgm_fx_cost_amt,
        dv_fx_net_price_amt,
        dv_fx_bgm_amt,
        internal_sbe_name,
        internal_sbe_descr,
        internal_be_name,
        internal_be_descr,
        external_sbe_name,
        external_sbe_descr,
        external_be_name,
        external_be_descr,
        dd_comp_us_list_price_amt,
        dd_extended_qty,
        dv_revenue_recognition_flg,
        adjustment_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        acquisition_order_origin_cd,
        acquisition_role,
        dv_ato_product_key
    FROM source_mt_rstd_bkgs_gross_margin
)

SELECT * FROM final
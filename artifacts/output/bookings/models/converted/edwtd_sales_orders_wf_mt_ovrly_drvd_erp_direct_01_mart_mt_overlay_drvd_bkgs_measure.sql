{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_ovrly_drvd_erp_direct', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_OVRLY_DRVD_ERP_DIRECT',
        'target_table': 'MT_OVERLAY_DRVD_BKGS_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.135175+00:00'
    }
) }}

WITH 

source_mt_overlay_drvd_bkgs_measure AS (
    SELECT
        bk_sls_terr_assignment_type_cd,
        overlay_bookings_measure_key,
        overlay_bookings_process_dt,
        ovrly_bkgs_msr_trans_type_cd,
        dv_forward_reverse_flg,
        dv_latest_sc_flg,
        dv_overlay_sales_rep_num,
        dv_overlay_sales_territory_key,
        dv_overlay_sales_cmsn_pct,
        pd_adjstmnt_rptng_type_cd,
        dv_overlay_sales_creation_dt,
        bookings_measure_key,
        ar_trx_line_key,
        bookings_process_dt,
        dv_fiscal_year_mth_number_int,
        bk_pos_transaction_id_int,
        dd_comp_us_net_price_amt,
        dd_comp_us_list_price_amt,
        dd_comp_us_cost_amt,
        dd_extended_qty,
        dv_sales_order_line_key,
        manual_trx_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        latest_dr_bkg_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_rep_annual_usd_amt,
        dv_annualized_us_net_amt,
        dv_multiyear_us_net_amt,
        sk_offer_attribution_id_int,
        sk_sales_motion_attrib_key,
        rtnr_unique_id,
        revenue_transfer_key,
        xaas_offer_atrbtn_rev_line_key,
        dv_ar_trx_line_key
    FROM {{ source('raw', 'mt_overlay_drvd_bkgs_measure') }}
),

final AS (
    SELECT
        bk_sls_terr_assignment_type_cd,
        overlay_bookings_measure_key,
        overlay_bookings_process_dt,
        ovrly_bkgs_msr_trans_type_cd,
        dv_forward_reverse_flg,
        dv_latest_sc_flg,
        dv_overlay_sales_rep_num,
        dv_overlay_sales_territory_key,
        dv_overlay_sales_cmsn_pct,
        pd_adjstmnt_rptng_type_cd,
        dv_overlay_sales_creation_dt,
        bookings_measure_key,
        ar_trx_line_key,
        bookings_process_dt,
        dv_fiscal_year_mth_number_int,
        bk_pos_transaction_id_int,
        dd_comp_us_net_price_amt,
        dd_comp_us_list_price_amt,
        dd_comp_us_cost_amt,
        dd_extended_qty,
        dv_sales_order_line_key,
        manual_trx_key,
        dv_so_sbscrptn_itm_sls_trx_key,
        latest_dr_bkg_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        source_rep_annual_usd_amt,
        dv_annualized_us_net_amt,
        dv_multiyear_us_net_amt,
        sk_offer_attribution_id_int,
        sk_sales_motion_attrib_key,
        rtnr_unique_id,
        revenue_transfer_key
    FROM source_mt_overlay_drvd_bkgs_measure
)

SELECT * FROM final
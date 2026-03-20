{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pre_overlay_bkgs_ar_napld', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_PRE_OVERLAY_BKGS_AR_NAPLD',
        'target_table': 'WI_OVERLAY_BOOKINGS_AR_NAPLD_R',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.590689+00:00'
    }
) }}

WITH 

source_wi_overlay_bookings_ar_napld_r AS (
    SELECT
        bookings_measure_key,
        overlay_sales_creation_dt,
        bkgs_msr_trans_type_code,
        ar_trx_line_key,
        bk_pos_transaction_id_int,
        manual_trx_key,
        overlay_sales_territory_key,
        overlay_sales_rep_number,
        bookings_process_date,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        bk_sls_terr_assignment_type_cd,
        overlay_sales_commission_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_sales_order_line_key,
        so_sbscrptn_itm_sls_trx_key,
        latest_dr_bkgs_flg,
        forward_reverse_flg,
        latest_record_flg,
        dv_fiscal_year_mth_number_int,
        pd_adjstmnt_rptng_type_cd,
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        source_rep_annual_usd_amt,
        dv_annualized_us_net_amt,
        dv_multiyear_us_net_amt,
        sk_offer_attribution_id_int,
        revenue_transfer_key
    FROM {{ source('raw', 'wi_overlay_bookings_ar_napld_r') }}
),

source_wi_overlay_bookings_ar_napld_f AS (
    SELECT
        bookings_measure_key,
        overlay_sales_creation_dt,
        bkgs_msr_trans_type_code,
        ar_trx_line_key,
        bk_pos_transaction_id_int,
        manual_trx_key,
        overlay_sales_territory_key,
        overlay_sales_rep_number,
        bookings_process_date,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        bk_sls_terr_assignment_type_cd,
        overlay_sales_commission_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_sales_order_line_key,
        so_sbscrptn_itm_sls_trx_key,
        latest_dr_bkgs_flg,
        forward_reverse_flg,
        latest_record_flg,
        dv_fiscal_year_mth_number_int,
        pd_adjstmnt_rptng_type_cd,
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        source_rep_annual_usd_amt,
        dv_annualized_us_net_amt,
        dv_multiyear_us_net_amt,
        sk_offer_attribution_id_int,
        revenue_transfer_key
    FROM {{ source('raw', 'wi_overlay_bookings_ar_napld_f') }}
),

final AS (
    SELECT
        bookings_measure_key,
        overlay_sales_creation_dt,
        bkgs_msr_trans_type_code,
        ar_trx_line_key,
        bk_pos_transaction_id_int,
        manual_trx_key,
        overlay_sales_territory_key,
        overlay_sales_rep_number,
        bookings_process_date,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        bk_sls_terr_assignment_type_cd,
        overlay_sales_commission_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_sales_order_line_key,
        so_sbscrptn_itm_sls_trx_key,
        latest_dr_bkgs_flg,
        forward_reverse_flg,
        latest_record_flg,
        dv_fiscal_year_mth_number_int,
        pd_adjstmnt_rptng_type_cd,
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        source_rep_annual_usd_amt,
        dv_annualized_us_net_amt,
        dv_multiyear_us_net_amt,
        sk_offer_attribution_id_int,
        revenue_transfer_key
    FROM source_wi_overlay_bookings_ar_napld_f
)

SELECT * FROM final
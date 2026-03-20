{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_r_disti_inv_month_measure', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_R_DISTI_INV_MONTH_MEASURE',
        'target_table': 'R_DISTI_INV_MONTH_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.758335+00:00'
    }
) }}

WITH 

source_n_disti_inventory_trx_line AS (
    SELECT
        disti_invntry_trx_line_id_int,
        on_hand_qty,
        committed_qty,
        on_order_qty,
        disti_reported_in_transit_qty,
        bk_wips_originator_id_int,
        product_key,
        disti_reported_disti_whse_num,
        local_currency_cd,
        glbl_prc_lst_unit_prc_usd_amt,
        glbl_prc_lst_unit_prc_lcl_amt,
        whlsl_prc_lst_unit_prc_usd_amt,
        whlsl_prc_lst_unit_prc_lcl_amt,
        disti_rprtd_unit_prc_usd_amt,
        disti_rprtd_unit_prc_lcl_amt,
        distributor_warehouse_num,
        valuation_price_usd_amt,
        valuation_price_local_amt,
        invntry_valuation_usd_amt,
        invntry_in_trnst_vltn_usd_amt,
        batch_id_int,
        inventory_as_of_dt,
        inventory_type_cd,
        second_source_flg,
        backordered_qty,
        second_source_iso_country_cd,
        last_update_dtm,
        vendor_part_num,
        local_to_usd_curr_conv_rt,
        avg_selling_price_usd_amt,
        avg_selling_price_local_amt,
        valuation_price_source_cd,
        active_flg,
        cisco_to_disti_in_transit_qty,
        dv_start_active_dt,
        dv_end_active_dt,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        obsolete_qty,
        distributor_rprtd_in_stock_qty,
        inventory_valuation_local_amt,
        inventory_om_avg_local_amt,
        inventory_om_avg_usd_amt,
        om_avg_local_amt,
        om_avg_usd_amt
    FROM {{ source('raw', 'n_disti_inventory_trx_line') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        product_key,
        distributor_id,
        qty_on_hand,
        committed_qty,
        qty_on_order,
        disti_in_transit_qty,
        cisco_disti_intransit_qty,
        valuation_price,
        inv_location,
        inv_valuation_amt,
        disti_in_transit_valuation_amt,
        inv_on_hand_amt,
        inv_as_of_date,
        dv_disti_rptd_net_prc_usd_amt
    FROM source_n_disti_inventory_trx_line
)

SELECT * FROM final
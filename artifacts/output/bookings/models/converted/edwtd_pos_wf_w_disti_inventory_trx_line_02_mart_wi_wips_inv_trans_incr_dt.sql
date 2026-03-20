{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_disti_inventory_trx_line', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTI_INVENTORY_TRX_LINE',
        'target_table': 'WI_WIPS_INV_TRANS_INCR_DT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.014238+00:00'
    }
) }}

WITH 

source_wi_wips_inv_trans_incr_dt AS (
    SELECT
        inventory_type_cd,
        bk_wips_originator_id_int,
        inventory_as_of_dt,
        dv_start_active_dt,
        dv_end_active_dt
    FROM {{ source('raw', 'wi_wips_inv_trans_incr_dt') }}
),

source_wi_wips_inv_trans_all AS (
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
        action_code,
        obsolete_qty,
        distributor_rprtd_in_stock_qty,
        inventory_valuation_local_amt,
        inventory_om_avg_local_amt,
        inventory_om_avg_usd_amt,
        om_avg_local_amt,
        om_avg_usd_amt
    FROM {{ source('raw', 'wi_wips_inv_trans_all') }}
),

final AS (
    SELECT
        inventory_type_cd,
        bk_wips_originator_id_int,
        inventory_as_of_dt,
        dv_start_active_dt,
        dv_end_active_dt
    FROM source_wi_wips_inv_trans_all
)

SELECT * FROM final
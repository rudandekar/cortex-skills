{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drvd_nrt_debookings_trx', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_WI_DRVD_NRT_DEBOOKINGS_TRX',
        'target_table': 'WI_LINES_INCR_DEBOOKINGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.409811+00:00'
    }
) }}

WITH 

source_wi_drvd_nrt_debookings_trx AS (
    SELECT
        conversion_dt,
        sales_rep_number,
        transaction_datetime,
        transaction_sequence_id_int,
        sales_channel_code,
        cdb_data_source_code,
        split_percentage,
        dd_service_flag,
        sold_to_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sales_order_line_key,
        bk_so_number_int,
        purchase_order_number,
        bk_so_src_crt_datetime,
        forward_reverse_code,
        bookings_percentage,
        net_price_amount,
        transaction_quantity,
        bk_iso_currency_code,
        sales_order_key,
        dd_item_type_code_flag,
        dd_rma_flag,
        dd_international_demo_flag,
        dd_replacement_demo_flag,
        dd_revenue_flag,
        dd_overlay_flag,
        dd_salesrep_flag,
        dd_ic_revenue_flag,
        dd_charges_flag,
        dd_misc_flag,
        dd_acquisition_flag,
        es_line_seq_id_int,
        cancelled_flg,
        ru_cisco_booked_datetime,
        sales_order_category_type,
        source_system_code,
        sales_order_operating_unit,
        product_key,
        sales_territory_key,
        sales_credit_type_code,
        customer_po_type_cd,
        shipment_priority_code,
        bk_so_source_name,
        pricing_date,
        line_creation_date,
        unit_list_price,
        sales_order_type_name,
        ep_changed_quantity,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        order_dtm,
        so_source_commit_dt,
        sol_source_commit_dt,
        sca_source_commit_dt,
        sales_credit_asgn_key,
        ep_process_dt,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dv_sales_order_line_key,
        dv_product_key,
        bk_so_line_cancel_reason_cd,
        cancel_code,
        attribution_pct,
        pob_type_cd,
        unit_whlsl_prc_lst_loc_amt
    FROM {{ source('raw', 'wi_drvd_nrt_debookings_trx') }}
),

source_n_cancelled_sol AS (
    SELECT
        sales_order_line_key,
        sol_cancel_reason_code,
        sol_cancelled_datetime,
        sol_cancelled_by_int,
        sol_cancelled_quantity,
        sol_order_quantity,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        bk_ship_set_num_int,
        sales_order_key,
        bk_so_line_cancel_reason_cd
    FROM {{ source('raw', 'n_cancelled_sol') }}
),

final AS (
    SELECT
        sales_order_line_key,
        source_commit_dtm,
        ep_process_dt,
        ss_cd
    FROM source_n_cancelled_sol
)

SELECT * FROM final
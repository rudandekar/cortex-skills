{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_shipset_line_nrt', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_WI_SHIPSET_LINE_NRT',
        'target_table': 'WI_SHIPSET_LINE_NRT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.436006+00:00'
    }
) }}

WITH 

source_n_sales_order_line_nrt_hist_tv AS (
    SELECT
        sales_order_line_key,
        ep_header_id_int,
        sales_order_key,
        order_qty,
        unit_net_price_local_amt,
        bookings_pct,
        sk_sales_order_line_id_int,
        ss_cd,
        product_key,
        ep_product_inv_item_id_int,
        ship_to_customer_key,
        ep_stc_ship_to_org_id_int,
        so_line_source_update_dtm,
        dv_so_line_source_update_dt,
        source_commit_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        unit_whlsl_prc_lst_loc_amt
    FROM {{ source('raw', 'n_sales_order_line_nrt_hist_tv') }}
),

final AS (
    SELECT
        sales_order_line_key,
        ru_shipset_num_int
    FROM source_n_sales_order_line_nrt_hist_tv
)

SELECT * FROM final
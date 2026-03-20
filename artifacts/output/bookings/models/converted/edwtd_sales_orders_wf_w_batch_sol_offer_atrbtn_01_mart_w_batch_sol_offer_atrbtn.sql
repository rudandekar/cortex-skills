{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_batch_sol_offer_atrbtn', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_BATCH_SOL_OFFER_ATRBTN',
        'target_table': 'W_BATCH_SOL_OFFER_ATRBTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.876521+00:00'
    }
) }}

WITH 

source_n_sol_offer_attribution_tv AS (
    SELECT
        sk_offer_attribution_id_int,
        ss_cd,
        sales_order_line_key,
        attributed_product_key,
        svc_contract_line_start_dt,
        svc_contract_line_end_dt,
        attribution_pct,
        cisco_booked_attribution_flg,
        unit_selling_prc_trx_amt,
        bookings_pct,
        product_attributed_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        exception_reason_cd,
        exception_status_cd,
        attribution_top_sku_flg,
        unit_list_price_usd_amt,
        top_sku_sales_order_line_key,
        attribution_cd,
        exception_detail_descr,
        enterprise_sku_prdt_key,
        sk_attribution_id_int
    FROM {{ source('raw', 'n_sol_offer_attribution_tv') }}
),

final AS (
    SELECT
        sk_offer_attribution_id_int,
        ss_cd,
        sales_order_line_key,
        attributed_product_key,
        svc_contract_line_start_dt,
        svc_contract_line_end_dt,
        attribution_pct,
        cisco_booked_attribution_flg,
        unit_selling_prc_trx_amt,
        bookings_pct,
        product_attributed_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        exception_reason_cd,
        exception_status_cd,
        attribution_top_sku_flg,
        unit_list_price_usd_amt,
        top_sku_sales_order_line_key,
        attribution_cd,
        exception_detail_descr,
        enterprise_sku_prdt_key,
        sk_attribution_id_int,
        action_code,
        dml_type
    FROM source_n_sol_offer_attribution_tv
)

SELECT * FROM final
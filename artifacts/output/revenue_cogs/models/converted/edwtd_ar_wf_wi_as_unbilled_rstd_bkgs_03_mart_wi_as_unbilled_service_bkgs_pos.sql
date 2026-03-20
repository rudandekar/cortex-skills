{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_unbilled_rstd_bkgs', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_AS_UNBILLED_RSTD_BKGS',
        'target_table': 'WI_AS_UNBILLED_SERVICE_BKGS_POS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.377041+00:00'
    }
) }}

WITH 

source_wi_as_unbilled_service_bkgs_neg AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        sales_territory_key,
        sales_rep_number,
        sales_order_key,
        sales_order_line_key,
        product_key,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        dv_sales_order_line_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dd_comp_us_net_price_amt
    FROM {{ source('raw', 'wi_as_unbilled_service_bkgs_neg') }}
),

source_wi_as_unbilled_rstd_bkgs AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        sales_territory_key,
        sales_rep_number,
        sales_order_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        product_key
    FROM {{ source('raw', 'wi_as_unbilled_rstd_bkgs') }}
),

source_wi_as_unbilled_service_bkgs_pos AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        sales_territory_key,
        sales_rep_number,
        sales_order_key,
        sales_order_line_key,
        product_key,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        dv_sales_order_line_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dd_comp_us_net_price_amt
    FROM {{ source('raw', 'wi_as_unbilled_service_bkgs_pos') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        sales_territory_key,
        sales_rep_number,
        sales_order_key,
        sales_order_line_key,
        product_key,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        dv_sales_order_line_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dd_comp_us_net_price_amt
    FROM source_wi_as_unbilled_service_bkgs_pos
)

SELECT * FROM final
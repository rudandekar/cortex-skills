{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_unbilled_rstd_bkgs_tss', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_UNBILLED_RSTD_BKGS_TSS',
        'target_table': 'WI_UNBILLED_RSTD_BKGS_TSS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.645584+00:00'
    }
) }}

WITH 

source_wi_unbilled_rstd_bkgs_tss AS (
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
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_unbilled_rstd_bkgs_tss') }}
),

source_wi_unbilled_service_bkgs_neg AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        sales_territory_key,
        sales_rep_number,
        sales_order_key,
        sales_order_line_key,
        goods_product_key,
        dv_sales_order_line_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dd_comp_us_net_price_amt
    FROM {{ source('raw', 'wi_unbilled_service_bkgs_neg') }}
),

source_wi_unbilled_service_bkgs_pos AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        sales_territory_key,
        sales_rep_number,
        sales_order_key,
        sales_order_line_key,
        goods_product_key,
        dv_sales_order_line_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dd_comp_us_net_price_amt
    FROM {{ source('raw', 'wi_unbilled_service_bkgs_pos') }}
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
        dv_sales_order_line_key,
        dv_attribution_cd,
        sk_offer_attribution_id_int
    FROM source_wi_unbilled_service_bkgs_pos
)

SELECT * FROM final
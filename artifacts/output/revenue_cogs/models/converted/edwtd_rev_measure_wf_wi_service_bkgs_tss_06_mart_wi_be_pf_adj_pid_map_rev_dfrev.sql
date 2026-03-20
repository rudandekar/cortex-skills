{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_service_bkgs_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SERVICE_BKGS_TSS',
        'target_table': 'WI_BE_PF_ADJ_PID_MAP_REV_DFREV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.502519+00:00'
    }
) }}

WITH 

source_wi_service_bkgs_pos_tss AS (
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
    FROM {{ source('raw', 'wi_service_bkgs_pos_tss') }}
),

source_wi_be_pf_adj_pid_map_rev_dfrev AS (
    SELECT
        business_entity_descr,
        bk_product_family_id,
        bk_prdt_allctn_clsfctn_cd,
        bk_product_id,
        item_key
    FROM {{ source('raw', 'wi_be_pf_adj_pid_map_rev_dfrev') }}
),

source_wi_service_bkgs_neg_tss AS (
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
    FROM {{ source('raw', 'wi_service_bkgs_neg_tss') }}
),

source_wi_rstd_bkgs_tss AS (
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
    FROM {{ source('raw', 'wi_rstd_bkgs_tss') }}
),

source_wi_rev_acq AS (
    SELECT
        src_entity_name,
        acqui_sku
    FROM {{ source('raw', 'wi_rev_acq') }}
),

source_wi_smt_rev_sku_exclude_tss AS (
    SELECT
        service_sku
    FROM {{ source('raw', 'wi_smt_rev_sku_exclude_tss') }}
),

final AS (
    SELECT
        business_entity_descr,
        bk_product_family_id,
        bk_prdt_allctn_clsfctn_cd,
        bk_product_id,
        item_key
    FROM source_wi_smt_rev_sku_exclude_tss
)

SELECT * FROM final
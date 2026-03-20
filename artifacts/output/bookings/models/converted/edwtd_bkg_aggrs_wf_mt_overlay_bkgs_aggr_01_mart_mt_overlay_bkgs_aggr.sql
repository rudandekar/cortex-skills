{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_overlay_bkgs_aggr', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_OVERLAY_BKGS_AGGR',
        'target_table': 'MT_OVERLAY_BKGS_AGGR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.238696+00:00'
    }
) }}

WITH 

source_mt_overlay_bkgs_aggr AS (
    SELECT
        dv_product_key,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        overlay_sales_territory_key,
        overlay_sales_rep_number,
        service_flg,
        ovrly_bkgs_msr_trans_type_code,
        bk_sls_terr_assignment_type_cd,
        dv_wtd_bkgs_net_amt,
        dv_mtd_bkgs_net_amt,
        dv_qtd_bkgs_net_amt,
        dv_q1_bkgs_net_amt,
        dv_q2_bkgs_net_amt,
        dv_q3_bkgs_net_amt,
        dv_q4_bkgs_net_amt,
        dv_ytd_bkgs_net_amt,
        dv_prev_week_bkgs_net_amt,
        dv_prev_mth_bkgs_net_amt,
        dv_prev_qtr_bkgs_net_amt,
        dv_attribution_cd,
        dv_fmv_flg,
        corporate_bookings_flg,
        dv_net_spread_flg,
        dv_revenue_recognition_flg,
        dsv_flg,
        rd_dv_product_key,
        rd_end_customer_key,
        rd_bill_to_customer_key,
        rd_ship_to_customer_key,
        rd_sold_to_customer_key,
        rd_overlay_sales_territory_key
    FROM {{ source('raw', 'mt_overlay_bkgs_aggr') }}
),

final AS (
    SELECT
        dv_product_key,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        overlay_sales_territory_key,
        overlay_sales_rep_num,
        service_flg,
        ovrly_bkgs_msr_trans_type_cd,
        bk_sls_terr_assignment_type_cd,
        dv_wtd_bkgs_net_amt,
        dv_mtd_bkgs_net_amt,
        dv_qtd_bkgs_net_amt,
        dv_q1_bkgs_net_amt,
        dv_q2_bkgs_net_amt,
        dv_q3_bkgs_net_amt,
        dv_q4_bkgs_net_amt,
        dv_ytd_bkgs_net_amt,
        dv_prev_week_bkgs_net_amt,
        dv_prev_mth_bkgs_net_amt,
        dv_prev_qtr_bkgs_net_amt,
        dv_attribution_cd,
        dv_fmv_flg,
        corporate_bookings_flg,
        dv_net_spread_flg,
        dv_revenue_recognition_flg,
        dsv_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        rd_dv_product_key,
        rd_end_customer_key,
        rd_bill_to_customer_key,
        rd_ship_to_customer_key,
        rd_sold_to_customer_key,
        rd_overlay_sales_territory_key,
        recurring_offer_flg,
        bk_prdt_allctn_clsfctn_cd
    FROM source_mt_overlay_bkgs_aggr
)

SELECT * FROM final
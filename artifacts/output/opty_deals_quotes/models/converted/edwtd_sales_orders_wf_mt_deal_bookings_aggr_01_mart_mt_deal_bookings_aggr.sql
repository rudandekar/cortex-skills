{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_deal_bookings_aggr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_DEAL_BOOKINGS_AGGR',
        'target_table': 'MT_DEAL_BOOKINGS_AGGR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.894621+00:00'
    }
) }}

WITH 

source_mt_deal_bookings_aggr AS (
    SELECT
        bk_deal_id,
        sales_territory_key,
        sales_rep_name,
        product_key,
        bookings_process_dt,
        dv_fiscal_year_mth_num_int,
        fiscal_month_id,
        fiscal_year_quarter_number_int,
        bkgs_measure_trans_type_cd,
        service_flg,
        src_rprtd_gdnc_dscnt_usd_amt,
        dd_comp_us_net_price_usd_amt,
        dd_comp_us_list_price_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_deal_bookings_aggr') }}
),

final AS (
    SELECT
        bk_deal_id,
        sales_territory_key,
        sales_rep_name,
        product_key,
        bookings_process_dt,
        dv_fiscal_year_mth_num_int,
        fiscal_month_id,
        fiscal_year_quarter_number_int,
        bkgs_measure_trans_type_cd,
        service_flg,
        src_rprtd_gdnc_dscnt_usd_amt,
        dd_comp_us_net_price_usd_amt,
        dd_comp_us_list_price_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_deal_bookings_aggr
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pos_sales_chnnl_matrix', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_POS_SALES_CHNNL_MATRIX',
        'target_table': 'WI_POS_SALES_CHNNL_MATRIX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.211507+00:00'
    }
) }}

WITH 

source_wi_inc_pos_part_drop_ship AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        dv_bk_end_user_site_use_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        dd_grndparnt_partner_party_key,
        partner_country,
        dv_end_cust_country,
        drop_ship_party_site_key
    FROM {{ source('raw', 'wi_inc_pos_part_drop_ship') }}
),

source_n_day AS (
    SELECT
        bk_calendar_date,
        bk_fiscal_year_number_int,
        bk_fiscal_week_number_int,
        bk_calendar_week_start_date,
        bk_fiscal_calendar_code,
        dv_clndr_ytd_flag,
        dv_clndr_qtd_flag,
        dv_clndr_mtd_flag,
        dv_clndr_wtd_flag,
        dv_fiscal_ytd_flag,
        dv_fiscal_qtd_flag,
        dv_fiscal_mtd_flag,
        dv_fiscal_wtd_flag,
        bk_calendar_month_number_int,
        bk_calendar_year_int,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'n_day') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        partner_party_site_key,
        channel_drop_ship_flg,
        channel_bookings_flg,
        edw_create_user,
        edw_create_datetime,
        bk_wips_originator_id_int,
        ship_to_customer_key
    FROM source_n_day
)

SELECT * FROM final
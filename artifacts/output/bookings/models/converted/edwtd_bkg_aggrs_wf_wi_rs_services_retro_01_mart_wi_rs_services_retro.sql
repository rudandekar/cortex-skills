{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rs_services_retro', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_RS_SERVICES_RETRO',
        'target_table': 'WI_RS_SERVICES_RETRO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.290837+00:00'
    }
) }}

WITH 

source_wi_rs_services_retro AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        sales_territory_key,
        sales_rep_number,
        dv_fiscal_year_mth_number_int,
        dv_pid_alloc_pct,
        dv_tss_alctn_mthd_type_id_int,
        fiscal_year_quarter_number_int,
        goods_product_key,
        recurring_software_flg,
        rs_dd_comp_us_list_price_amount,
        rs_dd_comp_us_net_price_amount,
        rs_dd_extended_quantity,
        rs_dv_annualized_us_net_amt,
        rs_dv_multiyear_us_net_amt,
        rstd_del_record_flg,
        service_item_key,
        rs_dd_comp_us_standard_price_amt,
        edw_create_datetime,
        edw_update_datetime,
        transaction_ai_flg,
        ai_intent_type_cd,
        trx_ai_product_class_name
    FROM {{ source('raw', 'wi_rs_services_retro') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        sales_territory_key,
        sales_rep_number,
        dv_fiscal_year_mth_number_int,
        dv_pid_alloc_pct,
        dv_tss_alctn_mthd_type_id_int,
        fiscal_year_quarter_number_int,
        goods_product_key,
        recurring_software_flg,
        rs_dd_comp_us_list_price_amount,
        rs_dd_comp_us_net_price_amount,
        rs_dd_extended_quantity,
        rs_dv_annualized_us_net_amt,
        rs_dv_multiyear_us_net_amt,
        rstd_del_record_flg,
        service_item_key,
        rs_dd_comp_us_standard_price_amt,
        edw_create_datetime,
        edw_update_datetime,
        transaction_ai_flg,
        ai_intent_type_cd,
        trx_ai_product_class_name
    FROM source_wi_rs_services_retro
)

SELECT * FROM final
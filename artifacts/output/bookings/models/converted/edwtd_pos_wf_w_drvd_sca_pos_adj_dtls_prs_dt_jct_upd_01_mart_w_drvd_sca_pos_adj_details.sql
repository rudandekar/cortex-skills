{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_drvd_sca_pos_adj_dtls_prs_dt_jct_upd', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DRVD_SCA_POS_ADJ_DTLS_PRS_DT_JCT_UPD',
        'target_table': 'W_DRVD_SCA_POS_ADJ_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.397931+00:00'
    }
) }}

WITH 

source_w_drvd_sca_pos_adj_details AS (
    SELECT
        allocated_pos_adj_key,
        sk_record_number_int,
        sk_sequence_number_int,
        bk_sales_rep_number,
        sales_credit_type_code,
        sales_commission_percentage,
        adjustment_date,
        extended_list_price_usd_amount,
        extended_cost_price_usd_amount,
        extended_net_price_usd_amount,
        forward_reverse_code,
        bk_pos_transaction_id_int,
        distributor_offset_flag,
        sales_territory_key,
        adjustment_code,
        description,
        sales_channel_code,
        sales_channel_source_type,
        channel_booking_flag,
        sales_adjustment_datetime,
        service_booking_flag,
        bk_fiscal_calendar_code,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        dv_fiscal_year_mth_number_int,
        product_key,
        process_date,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_drvd_sca_pos_adj_details') }}
),

final AS (
    SELECT
        allocated_pos_adj_key,
        sk_record_number_int,
        sk_sequence_number_int,
        bk_sales_rep_number,
        sales_credit_type_code,
        sales_commission_percentage,
        adjustment_date,
        extended_list_price_usd_amount,
        extended_cost_price_usd_amount,
        extended_net_price_usd_amount,
        forward_reverse_code,
        bk_pos_transaction_id_int,
        distributor_offset_flag,
        sales_territory_key,
        adjustment_code,
        description,
        sales_channel_code,
        sales_channel_source_type,
        channel_booking_flag,
        sales_adjustment_datetime,
        service_booking_flag,
        bk_fiscal_calendar_code,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        dv_fiscal_year_mth_number_int,
        product_key,
        process_date,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dv_corporate_booking_flg,
        adjustment_qty,
        dv_latest_flg,
        action_code,
        dml_type
    FROM source_w_drvd_sca_pos_adj_details
)

SELECT * FROM final
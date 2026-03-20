{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_quote_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_W_AS_QUOTE_SEP',
        'target_table': 'W_AS_QUOTE_SEP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.008554+00:00'
    }
) }}

WITH 

source_w_as_quote_sep AS (
    SELECT
        bk_as_quote_num,
        quote_type,
        as_quote_status_cd,
        as_quote_create_dtm,
        dv_as_quote_create_dt,
        ss_cd,
        as_quote_created_by_party_key,
        as_quote_updated_by_party_key,
        deal_id,
        as_quote_updated_dtm,
        dv_as_quote_updated_dt,
        as_msp_region_name,
        as_msp_theater_name,
        ru_as_sbscrptn_quote_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        booked_role,
        ru_as_quote_booked_dt,
        as_qt_te_exclude_from_mrgn_flg,
        as_quote_name,
        bk_solution_name,
        list_price_net_dscnt_pct,
        quoting_sales_channel_name,
        quoting_end_cust_segment_name,
        dd_market_segment_name,
        bk_as_project_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_as_quote_sep') }}
),

final AS (
    SELECT
        bk_as_quote_num,
        quote_type,
        as_quote_status_cd,
        as_quote_create_dtm,
        dv_as_quote_create_dt,
        ss_cd,
        as_quote_created_by_party_key,
        as_quote_updated_by_party_key,
        deal_id,
        as_quote_updated_dtm,
        dv_as_quote_updated_dt,
        as_msp_region_name,
        as_msp_theater_name,
        ru_as_sbscrptn_quote_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        booked_role,
        ru_as_quote_booked_dt,
        as_qt_te_exclude_from_mrgn_flg,
        as_quote_name,
        bk_solution_name,
        list_price_net_dscnt_pct,
        quoting_sales_channel_name,
        quoting_end_cust_segment_name,
        dd_market_segment_name,
        bk_as_project_cd,
        action_code,
        dml_type
    FROM source_w_as_quote_sep
)

SELECT * FROM final
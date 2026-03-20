{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_quote_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QUOTE_HDP',
        'target_table': 'ST_AS_QUOTE_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.977568+00:00'
    }
) }}

WITH 

source_ff_as_quote_hdp AS (
    SELECT
        bk_as_quote_num,
        quote_type,
        bk_project_cd,
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
        market_segment_name,
        ru_as_sbscrptn_quote_type_cd,
        ru_as_quote_booked_dt,
        as_qt_te_exclude_from_mrgn_flg,
        as_quote_name,
        bk_solution_name,
        list_price_net_dscnt_pct,
        quoting_sales_channel_name,
        quoting_end_cust_segment_name
    FROM {{ source('raw', 'ff_as_quote_hdp') }}
),

transformed_exptrans AS (
    SELECT
    bk_as_quote_num,
    quote_type,
    bk_project_cd,
    as_quote_status_cd,
    as_quote_create_dtm_str,
    dv_as_quote_create_dt_str,
    ss_cd,
    as_quote_created_by_party_key,
    as_quote_updated_by_party_key,
    deal_id,
    as_quote_updated_dtm,
    dv_as_quote_updated_dt,
    as_msp_region_name,
    as_msp_theater_name,
    market_segment_name,
    ru_as_sbscrptn_quote_type_cd,
    ru_as_quote_booked_dt,
    as_qt_te_exclude_from_mrgn_flg,
    as_quote_name,
    bk_solution_name,
    list_price_net_dscnt_pct,
    quoting_sales_channel_name,
    quoting_end_cust_segment_name,
    TO_DATE(AS_QUOTE_CREATE_DTM_STR, 'yyyy-mm-dd hh24.mi.ss') AS as_quote_create_dtm_str1,
    TO_DATE(DV_AS_QUOTE_CREATE_DT_STR, 'yyyy-mm-dd') AS dv_as_quote_create_dt_str1,
    TO_DATE(AS_QUOTE_UPDATED_DTM, 'yyyy-mm-dd hh24.mi.ss') AS as_quote_updated_dtm1,
    TO_DATE(DV_AS_QUOTE_UPDATED_DT, 'yyyy-mm-dd') AS dv_as_quote_updated_dt1,
    TO_DATE(DECODE(RU_AS_QUOTE_BOOKED_DT, 'NULL', '35000101') , 'yyyymmdd') AS ru_as_quote_booked_dt1,
    CURRENT_TIMESTAMP() AS created_datetime
    FROM source_ff_as_quote_hdp
),

final AS (
    SELECT
        bk_as_quote_num,
        quote_type,
        bk_as_project_cd,
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
        dd_market_segment_name,
        ru_as_sbscrptn_quote_type_cd,
        ru_as_quote_booked_dt,
        as_qt_te_exclude_from_mrgn_flg,
        as_quote_name,
        bk_solution_name,
        list_price_net_dscnt_pct,
        quoting_sales_channel_name,
        quoting_end_cust_segment_name,
        created_dtm
    FROM transformed_exptrans
)

SELECT * FROM final
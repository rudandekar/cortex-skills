{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_gp_usr_weekly_forecast', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_GP_USR_WEEKLY_FORECAST',
        'target_table': 'EX_XXCMF_DP_USR_WEEKLY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.632043+00:00'
    }
) }}

WITH 

source_st_xxcmf_dp_usr_weekly AS (
    SELECT
        pid_code,
        customer_code,
        theater_code,
        sdate,
        usr_units_baseline_rev,
        usr_exceptions_rev,
        total_usr_rev,
        usr_units_baseline_nrev,
        usr_exceptions_nrev,
        total_usr_nrev,
        publish_date,
        etl_load_date,
        etl_source,
        naive_usr,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcmf_dp_usr_weekly') }}
),

source_w_gp_usr_weekly_forecast AS (
    SELECT
        goods_product_key,
        bk_fiscal_week_num_int,
        bk_fiscal_year_num_int,
        bk_fiscal_calendar_cd,
        forecast_publication_dt,
        total_usr_nonrevenue_usd_amt,
        total_usr_revenue_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_gp_usr_weekly_forecast') }}
),

final AS (
    SELECT
        pid_code,
        customer_code,
        theater_code,
        sdate,
        usr_units_baseline_rev,
        usr_exceptions_rev,
        total_usr_rev,
        usr_units_baseline_nrev,
        usr_exceptions_nrev,
        total_usr_nrev,
        publish_date,
        etl_load_date,
        etl_source,
        naive_usr,
        batch_id,
        create_datetime,
        action_code
    FROM source_w_gp_usr_weekly_forecast
)

SELECT * FROM final
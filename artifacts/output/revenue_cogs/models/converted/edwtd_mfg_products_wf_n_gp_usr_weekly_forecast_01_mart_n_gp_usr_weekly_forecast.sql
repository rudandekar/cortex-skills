{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_gp_usr_weekly_forecast', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_GP_USR_WEEKLY_FORECAST',
        'target_table': 'N_GP_USR_WEEKLY_FORECAST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.782681+00:00'
    }
) }}

WITH 

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
        edw_update_user
    FROM source_w_gp_usr_weekly_forecast
)

SELECT * FROM final
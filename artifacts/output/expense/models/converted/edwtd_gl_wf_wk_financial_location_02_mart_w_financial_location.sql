{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_location', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_LOCATION',
        'target_table': 'W_FINANCIAL_LOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.808345+00:00'
    }
) }}

WITH 

source_st_si_location_info AS (
    SELECT
        batch_id,
        location_value,
        location_name,
        company_value,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_location_info') }}
),

transformed_exp_w_financial_location AS (
    SELECT
    bk_company_code,
    bk_financial_location_code,
    location_name,
    sk_location_value_code,
    action_code,
    exception_type,
    start_tv_date,
    end_tv_date,
    rank_index,
    dml_type,
    'NE' AS error_check
    FROM source_st_si_location_info
),

transformed_ex_ex_si_location_info AS (
    SELECT
    batch_id,
    location_value,
    location_name,
    company_value,
    create_datetime,
    action_code,
    'RI' AS exception_type
    FROM transformed_exp_w_financial_location
),

final AS (
    SELECT
        bk_financial_location_code,
        start_tv_date,
        end_tv_date,
        location_name,
        bk_company_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_location_value_code,
        action_code,
        dml_type
    FROM transformed_ex_ex_si_location_info
)

SELECT * FROM final
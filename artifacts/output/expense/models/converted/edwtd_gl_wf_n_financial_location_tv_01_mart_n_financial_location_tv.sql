{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_location_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_LOCATION_TV',
        'target_table': 'N_FINANCIAL_LOCATION_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.821758+00:00'
    }
) }}

WITH 

source_w_financial_location AS (
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
    FROM {{ source('raw', 'w_financial_location') }}
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
        sk_location_value_code
    FROM source_w_financial_location
)

SELECT * FROM final
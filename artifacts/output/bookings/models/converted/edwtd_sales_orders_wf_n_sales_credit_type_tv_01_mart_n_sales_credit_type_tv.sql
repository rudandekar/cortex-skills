{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_credit_type_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_CREDIT_TYPE_TV',
        'target_table': 'N_SALES_CREDIT_TYPE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.922737+00:00'
    }
) }}

WITH 

source_w_sales_credit_type AS (
    SELECT
        bk_sales_credit_type_code,
        start_tv_date,
        end_tv_date,
        sales_credit_type_description,
        sales_credit_type_enabled_flag,
        sales_credit_type_quota_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_credit_type') }}
),

final AS (
    SELECT
        bk_sales_credit_type_code,
        start_tv_date,
        end_tv_date,
        sales_credit_type_description,
        sales_credit_type_enabled_flag,
        sales_credit_type_quota_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_w_sales_credit_type
)

SELECT * FROM final
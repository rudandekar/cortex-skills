{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_type_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_TYPE_TV',
        'target_table': 'N_SALES_ORDER_TYPE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.869159+00:00'
    }
) }}

WITH 

source_w_sales_order_type AS (
    SELECT
        bk_order_type_name,
        start_tv_date,
        end_tv_date,
        so_type_revenue_gen_flag,
        so_type_description,
        so_type_active_start_dtm,
        so_type_active_end_dtm,
        dv_sales_order_type_alt_name,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime,
        duty_billing_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_type') }}
),

final AS (
    SELECT
        bk_order_type_name,
        start_tv_date,
        end_tv_date,
        so_type_revenue_gen_flag,
        so_type_description,
        so_type_active_start_dtm,
        so_type_active_end_dtm,
        dv_sales_order_type_alt_name,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime,
        duty_billing_cd
    FROM source_w_sales_order_type
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_so_end_customer', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SO_END_CUSTOMER',
        'target_table': 'N_SO_END_CUSTOMER_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.088502+00:00'
    }
) }}

WITH 

source_w_so_end_customer AS (
    SELECT
        end_customer_key,
        dd_end_customer_type,
        sk_sales_order_header_id_int,
        ss_cd,
        sales_order_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_ssp_date,
        start_tv_datetime,
        end_ssp_date,
        end_tv_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_so_end_customer') }}
),

final AS (
    SELECT
        end_customer_key,
        dd_end_customer_type,
        sk_sales_order_header_id_int,
        ss_cd,
        sales_order_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_ssp_date,
        start_tv_datetime,
        end_ssp_date,
        end_tv_datetime
    FROM source_w_so_end_customer
)

SELECT * FROM final
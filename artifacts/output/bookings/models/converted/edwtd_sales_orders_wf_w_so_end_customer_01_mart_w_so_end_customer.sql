{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_so_end_customer', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SO_END_CUSTOMER',
        'target_table': 'W_SO_END_CUSTOMER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.613657+00:00'
    }
) }}

WITH 

source_wi_so_end_customer AS (
    SELECT
        sales_order_key,
        end_customer_key,
        end_customer_type_code,
        sk_sales_order_header_id_int,
        ss_code,
        edw_create_datetime
    FROM {{ source('raw', 'wi_so_end_customer') }}
),

transformed_exp_w_so_end_customer AS (
    SELECT
    sales_order_key,
    end_customer_key,
    end_customer_type_code,
    sk_sales_order_header_id_int,
    ss_code,
    start_tv_datetime,
    end_tv_datetime,
    rank_index,
    dml_type,
    edw_create_datetime,
    TO_DATE(TO_CHAR(START_TV_DATETIME ,'YYYYMMDD'),'YYYYMMDD') AS start_ssp_date,
    TO_DATE(TO_CHAR(END_TV_DATETIME ,'YYYYMMDD'),'YYYYMMDD') AS end_ssp_date
    FROM source_wi_so_end_customer
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
        end_tv_datetime,
        action_code,
        dml_type
    FROM transformed_exp_w_so_end_customer
)

SELECT * FROM final
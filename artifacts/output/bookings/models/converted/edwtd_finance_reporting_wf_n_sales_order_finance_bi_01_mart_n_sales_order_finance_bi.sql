{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_finance_bi', 'batch', 'edwtd_finance_reporting'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_FINANCE_BI',
        'target_table': 'N_SALES_ORDER_FINANCE_BI',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.497901+00:00'
    }
) }}

WITH 

source_w_sales_order_finance_bi AS (
    SELECT
        sales_order_key,
        dv_discount_band_cd,
        dv_order_band_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_finance_bi') }}
),

final AS (
    SELECT
        sales_order_key,
        dv_discount_band_cd,
        dv_order_band_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_sales_order_finance_bi
)

SELECT * FROM final
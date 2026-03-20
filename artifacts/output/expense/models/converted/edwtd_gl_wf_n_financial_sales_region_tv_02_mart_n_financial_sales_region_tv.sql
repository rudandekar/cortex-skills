{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_sales_region_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_SALES_REGION_TV',
        'target_table': 'N_FINANCIAL_SALES_REGION_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.064165+00:00'
    }
) }}

WITH 

source_n_financial_sales_region_tv AS (
    SELECT
        bk_financial_sales_region_id,
        start_tv_dt,
        end_tv_dt,
        financial_sales_region_descr,
        sk_region_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'n_financial_sales_region_tv') }}
),

source_w_financial_sales_region AS (
    SELECT
        bk_financial_sales_region_id,
        start_tv_dt,
        end_tv_dt,
        financial_sales_region_descr,
        sk_region_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_financial_sales_region') }}
),

final AS (
    SELECT
        bk_financial_sales_region_id,
        start_tv_dt,
        end_tv_dt,
        financial_sales_region_descr,
        sk_region_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_w_financial_sales_region
)

SELECT * FROM final
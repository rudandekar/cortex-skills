{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_sales_region', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_SALES_REGION',
        'target_table': 'W_FINANCIAL_SALES_REGION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.028646+00:00'
    }
) }}

WITH 

source_st_si_region AS (
    SELECT
        batch_id,
        region_id,
        region_value,
        enabled_flag,
        region_desc,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_region') }}
),

transformed_exp_w_financial_sales_region AS (
    SELECT
    batch_id,
    sk_region_id_int,
    financial_sales_region_id,
    financial_sales_region_descr,
    end_tv_date,
    start_tv_date,
    action_code,
    dml_type,
    rank_index
    FROM source_st_si_region
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
        edw_update_dtm,
        action_code,
        dml_type
    FROM transformed_exp_w_financial_sales_region
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_tax_category_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_TAX_CATEGORY_TV',
        'target_table': 'N_FINANCIAL_TAX_CATEGORY_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.674259+00:00'
    }
) }}

WITH 

source_w_financial_tax_category AS (
    SELECT
        bk_financial_tax_category_id,
        start_tv_dt,
        end_tv_dt,
        financial_tax_category_descr,
        sk_tax_category_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_financial_tax_category') }}
),

source_n_financial_tax_category_tv AS (
    SELECT
        bk_financial_tax_category_id,
        start_tv_dt,
        end_tv_dt,
        financial_tax_category_descr,
        sk_tax_category_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'n_financial_tax_category_tv') }}
),

final AS (
    SELECT
        bk_financial_tax_category_id,
        start_tv_dt,
        end_tv_dt,
        financial_tax_category_descr,
        sk_tax_category_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_n_financial_tax_category_tv
)

SELECT * FROM final
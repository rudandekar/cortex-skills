{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_tax_category', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_TAX_CATEGORY',
        'target_table': 'W_FINANCIAL_TAX_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.878600+00:00'
    }
) }}

WITH 

source_st_si_tax_category AS (
    SELECT
        batch_id,
        tax_category_id,
        tax_category_value,
        tax_category_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_tax_category') }}
),

transformed_exp_financial_tax_category AS (
    SELECT
    batch_id,
    financial_tax_category_descr,
    financial_tax_category_id,
    start_date_active,
    sk_tax_category_id_int,
    end_date_active,
    action_code,
    rank_index,
    dml_type
    FROM source_st_si_tax_category
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
        edw_update_dtm,
        action_code,
        dml_type
    FROM transformed_exp_financial_tax_category
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_project_category', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_PROJECT_CATEGORY',
        'target_table': 'N_FINANCIAL_PROJECT_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.023585+00:00'
    }
) }}

WITH 

source_w_financial_project_category AS (
    SELECT
        bk_fin_proj_category_id_int,
        financial_proj_category_descr,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_financial_project_category') }}
),

final AS (
    SELECT
        bk_fin_proj_category_id_int,
        financial_proj_category_descr,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_w_financial_project_category
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_project_category', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_PROJECT_CATEGORY',
        'target_table': 'W_FINANCIAL_PROJECT_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.125946+00:00'
    }
) }}

WITH 

source_st_si_project_category AS (
    SELECT
        batch_id,
        project_category_id,
        project_category_desc,
        enabled_flag,
        si_flex_struct_id,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_project_category') }}
),

transformed_exp_w_fin_project_category AS (
    SELECT
    batch_id,
    project_category_id,
    project_category_desc,
    enabled_flag,
    si_flex_struct_id,
    last_update_date,
    create_datetime,
    action_code
    FROM source_st_si_project_category
),

final AS (
    SELECT
        bk_fin_proj_category_id_int,
        financial_proj_category_descr,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM transformed_exp_w_fin_project_category
)

SELECT * FROM final
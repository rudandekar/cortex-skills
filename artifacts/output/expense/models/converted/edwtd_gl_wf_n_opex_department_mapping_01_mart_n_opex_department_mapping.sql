{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_opex_department_mapping', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_OPEX_DEPARTMENT_MAPPING',
        'target_table': 'N_OPEX_DEPARTMENT_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.131826+00:00'
    }
) }}

WITH 

source_w_opex_department_mapping AS (
    SELECT
        bk_company_code,
        bk_department_code,
        bk_functional_opex_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        restated_fiscal_yr_num_int,
        restated_fiscal_mth_num_int,
        restatement_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_opex_department_mapping') }}
),

final AS (
    SELECT
        bk_company_code,
        bk_department_code,
        bk_functional_opex_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        restated_fiscal_yr_num_int,
        restated_fiscal_mth_num_int,
        restatement_flg
    FROM source_w_opex_department_mapping
)

SELECT * FROM final
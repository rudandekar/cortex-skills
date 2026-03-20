{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_opex_department_mapping', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_OPEX_DEPARTMENT_MAPPING',
        'target_table': 'W_OPEX_DEPARTMENT_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.162839+00:00'
    }
) }}

WITH 

source_st_ae_opxaloc_dept_opex_map AS (
    SELECT
        opex_name,
        company_cd,
        department_cd,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        rstd_fiscal_month_id,
        rstd_flag
    FROM {{ source('raw', 'st_ae_opxaloc_dept_opex_map') }}
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
        action_code,
        dml_type,
        restatement_flg,
        restated_fiscal_yr_num_int,
        restated_fiscal_mth_num_int
    FROM source_st_ae_opxaloc_dept_opex_map
)

SELECT * FROM final
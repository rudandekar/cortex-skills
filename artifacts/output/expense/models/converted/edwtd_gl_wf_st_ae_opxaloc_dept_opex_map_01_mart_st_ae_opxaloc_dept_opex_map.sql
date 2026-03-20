{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_opxaloc_dept_opex_map', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_AE_OPXALOC_DEPT_OPEX_MAP',
        'target_table': 'ST_AE_OPXALOC_DEPT_OPEX_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.712203+00:00'
    }
) }}

WITH 

source_ff_ae_opxaloc_dept_opex_map AS (
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
    FROM {{ source('raw', 'ff_ae_opxaloc_dept_opex_map') }}
),

final AS (
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
    FROM source_ff_ae_opxaloc_dept_opex_map
)

SELECT * FROM final
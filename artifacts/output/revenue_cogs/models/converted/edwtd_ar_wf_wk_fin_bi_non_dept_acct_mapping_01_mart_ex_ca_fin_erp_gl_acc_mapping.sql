{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_fin_bi_non_dept_acct_mapping', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_FIN_BI_NON_DEPT_ACCT_MAPPING',
        'target_table': 'EX_CA_FIN_ERP_GL_ACC_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.217232+00:00'
    }
) }}

WITH 

source_el_ca_fin_profit_center_map AS (
    SELECT
        department_name,
        profit_center
    FROM {{ source('raw', 'el_ca_fin_profit_center_map') }}
),

source_st_ca_fin_erp_gl_acc_mapping AS (
    SELECT
        account_num,
        description,
        level_1,
        level_2,
        level_3,
        level_4
    FROM {{ source('raw', 'st_ca_fin_erp_gl_acc_mapping') }}
),

final AS (
    SELECT
        account_num,
        description,
        level_1,
        level_2,
        level_3,
        level_4
    FROM source_st_ca_fin_erp_gl_acc_mapping
)

SELECT * FROM final
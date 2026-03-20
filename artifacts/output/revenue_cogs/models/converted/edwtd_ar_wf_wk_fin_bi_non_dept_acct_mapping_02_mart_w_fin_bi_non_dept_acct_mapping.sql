{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_fin_bi_non_dept_acct_mapping', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_FIN_BI_NON_DEPT_ACCT_MAPPING',
        'target_table': 'W_FIN_BI_NON_DEPT_ACCT_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.374236+00:00'
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
        bk_financial_account_cd,
        bk_profit_center_name,
        level_1_cd,
        level_2_cd,
        level_3_cd,
        level_4_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ca_fin_erp_gl_acc_mapping
)

SELECT * FROM final
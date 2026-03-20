{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_bi_non_dept_acct_mapping', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_FIN_BI_NON_DEPT_ACCT_MAPPING',
        'target_table': 'N_FIN_BI_NON_DEPT_ACCT_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.012889+00:00'
    }
) }}

WITH 

source_w_fin_bi_non_dept_acct_mapping AS (
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
    FROM {{ source('raw', 'w_fin_bi_non_dept_acct_mapping') }}
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
        edw_update_user
    FROM source_w_fin_bi_non_dept_acct_mapping
)

SELECT * FROM final
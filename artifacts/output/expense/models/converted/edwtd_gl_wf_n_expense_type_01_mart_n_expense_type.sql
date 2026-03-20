{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_expense_type', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EXPENSE_TYPE',
        'target_table': 'N_EXPENSE_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.757481+00:00'
    }
) }}

WITH 

source_w_expense_type AS (
    SELECT
        bk_expense_type_name,
        policy_threshold_usd_amt,
        policy_classification_name,
        procurement_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_expense_type') }}
),

final AS (
    SELECT
        bk_expense_type_name,
        policy_threshold_usd_amt,
        policy_classification_name,
        procurement_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_expense_type
)

SELECT * FROM final
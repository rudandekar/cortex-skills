{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_expense_line_svc_prvdr_stg23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EXPENSE_LINE_SVC_PRVDR_STG23NF',
        'target_table': 'N_EXPENSE_LINE_SVC_PRVDR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.916861+00:00'
    }
) }}

WITH 

source_st_expense_line_svc_prvdr AS (
    SELECT
        expense_ln_svc_prvdr_name,
        source_deleted_flg,
        action_cd,
        create_datetime
    FROM {{ source('raw', 'st_expense_line_svc_prvdr') }}
),

final AS (
    SELECT
        bk_expense_ln_svc_prvdr_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_expense_line_svc_prvdr
)

SELECT * FROM final
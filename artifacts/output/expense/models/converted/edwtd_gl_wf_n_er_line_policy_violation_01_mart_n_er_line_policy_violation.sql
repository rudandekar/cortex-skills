{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_er_line_policy_violation', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_ER_LINE_POLICY_VIOLATION',
        'target_table': 'N_ER_LINE_POLICY_VIOLATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.914369+00:00'
    }
) }}

WITH 

source_w_er_line_policy_violation AS (
    SELECT
        policy_violation_type_cd,
        sk_report_header_id,
        sk_distribution_line_num_int,
        sk_violation_num_int,
        ss_cd,
        er_line_policy_violation_key,
        violation_dt,
        allowable_amt,
        exceeded_amt,
        expense_report_line_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type,
        source_deleted_flg
    FROM {{ source('raw', 'w_er_line_policy_violation') }}
),

final AS (
    SELECT
        policy_violation_type_cd,
        sk_report_header_id,
        sk_distribution_line_num_int,
        sk_violation_num_int,
        ss_cd,
        er_line_policy_violation_key,
        violation_dt,
        allowable_amt,
        exceeded_amt,
        expense_report_line_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        source_deleted_flg
    FROM source_w_er_line_policy_violation
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_posting_control', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_POSTING_CONTROL',
        'target_table': 'N_AR_POSTING_CONTROL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.554403+00:00'
    }
) }}

WITH 

source_w_ar_posting_control AS (
    SELECT
        ar_posting_control_key,
        gl_posted_dt,
        posting_control_dtm,
        sk_gl_posting_control_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_posting_control') }}
),

final AS (
    SELECT
        ar_posting_control_key,
        gl_posted_dt,
        posting_control_dtm,
        sk_gl_posting_control_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ar_posting_control
)

SELECT * FROM final
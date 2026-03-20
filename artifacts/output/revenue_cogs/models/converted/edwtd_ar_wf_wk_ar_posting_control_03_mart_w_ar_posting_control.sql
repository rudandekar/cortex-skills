{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ar_posting_control', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_AR_POSTING_CONTROL',
        'target_table': 'W_AR_POSTING_CONTROL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.960945+00:00'
    }
) }}

WITH 

source_n_source_system_codes AS (
    SELECT
        source_system_code,
        source_system_name,
        database_name,
        company,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user,
        global_name,
        gmt_offset
    FROM {{ source('raw', 'n_source_system_codes') }}
),

source_sm_ar_posting_control AS (
    SELECT
        ar_posting_control_key,
        sk_gl_posting_control_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_ar_posting_control') }}
),

source_st_om_ar_posting_control AS (
    SELECT
        argcgp_request_id,
        argltp_request_id,
        code_combination_id_gain,
        code_combination_id_loss,
        created_by,
        creation_date,
        ges_update_date,
        gllezl_request_id,
        global_name,
        gl_posted_date,
        interface_run_id,
        posting_control_id,
        post_thru_date,
        program_application_id,
        program_id,
        program_update_date,
        report_only_flag,
        request_id,
        run_gl_journal_import_flag,
        start_date,
        status,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_ar_posting_control') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_om_ar_posting_control
)

SELECT * FROM final
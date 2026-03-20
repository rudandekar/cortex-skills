{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_coo_prjct_rule_q_a', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_COO_PRJCT_RULE_Q_A',
        'target_table': 'EX_CO_PROJECT_DT_ANS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.946839+00:00'
    }
) }}

WITH 

source_ex_co_project_dt_ans AS (
    SELECT
        batch_id,
        project_dt_id,
        project_id,
        seq_id,
        step_id,
        answer,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        dt_revision_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_co_project_dt_ans') }}
),

source_st_co_project_dt_ans AS (
    SELECT
        batch_id,
        project_dt_id,
        project_id,
        seq_id,
        step_id,
        answer,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        dt_revision_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_co_project_dt_ans') }}
),

source_st_co_project_dt AS (
    SELECT
        batch_id,
        project_dt_id,
        project_id,
        seq_id,
        step_id,
        answer,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        dt_revision_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_co_project_dt') }}
),

source_w_coo_prjct_rule_q_a AS (
    SELECT
        bk_coo_prjct_name,
        bk_coo_prjct_rule_q_a_rvsn_int,
        bk_coo_prjct_rule_q_a_sqnc_int,
        coo_prjct_q_a_qstn_num_int,
        coo_prjct_q_a_qstn_txt,
        coo_prjct_q_a_answer_txt,
        conclusion_type,
        created_by_dtm,
        dv_created_by_dt,
        last_upd_csco_wrkr_pty_key,
        last_updated_on_dtm,
        dv_last_updated_on_dt,
        source_deleted_flg,
        created_csco_wrkr_pty_key,
        ru_coo_prjct_q_a_cnclsn_rl_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_coo_prjct_rule_q_a') }}
),

source_ex_co_project_dt AS (
    SELECT
        batch_id,
        project_dt_id,
        project_id,
        seq_id,
        step_id,
        answer,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        dt_revision_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_co_project_dt') }}
),

final AS (
    SELECT
        batch_id,
        project_dt_id,
        project_id,
        seq_id,
        step_id,
        answer,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        dt_revision_id,
        create_datetime,
        action_code,
        exception_type
    FROM source_ex_co_project_dt
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_coo_prjct_rule_q_a_tv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_COO_PRJCT_RULE_Q_A_TV',
        'target_table': 'N_COO_PRJCT_RULE_Q_A_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.727604+00:00'
    }
) }}

WITH 

source_n_coo_prjct_rule_q_a_tv AS (
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
        end_tv_dt
    FROM {{ source('raw', 'n_coo_prjct_rule_q_a_tv') }}
),

source_n_coo_prjct_rule_q_a AS (
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
        edw_update_user
    FROM {{ source('raw', 'n_coo_prjct_rule_q_a') }}
),

final AS (
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
        end_tv_dt
    FROM source_n_coo_prjct_rule_q_a
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_quote_milestone_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_AS_QUOTE_MILESTONE_SEP',
        'target_table': 'N_AS_QUOTE_MILESTONE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.169145+00:00'
    }
) }}

WITH 

source_w_as_quote_milestone_sep AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        as_quote_milestone_name,
        as_qt_milestone_te_mrgn_flg,
        as_qt_milestone_active_sts_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        rev_planning_duration_days_cnt,
        as_milestone_start_dt,
        master_milestone_flg,
        action_code,
        dml_type,
        ss_cd
    FROM {{ source('raw', 'w_as_quote_milestone_sep') }}
),

final AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        as_quote_milestone_name,
        as_qt_milestone_te_mrgn_flg,
        as_qt_milestone_active_sts_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        rev_planning_duration_days_cnt,
        as_milestone_start_dt,
        master_milestone_flg,
        bk_ss_cd
    FROM source_w_as_quote_milestone_sep
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_quote_milestone_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QUOTE_MILESTONE_HDP',
        'target_table': 'ST_AS_QUOTE_MILESTONE_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.087850+00:00'
    }
) }}

WITH 

source_ff_as_quote_milestone_hdp AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        as_quote_milestone_name,
        as_qt_milestone_te_mrgn_flg,
        as_qt_milestone_active_sts_cd,
        rev_planning_duration_days_cnt,
        as_milestone_start_dt,
        master_milestone_flg,
        ss_cd
    FROM {{ source('raw', 'ff_as_quote_milestone_hdp') }}
),

transformed_exptrans AS (
    SELECT
    bk_as_quote_num,
    bk_as_quote_milestone_num_int_str,
    as_quote_milestone_name,
    as_qt_milestone_te_mrgn_flg,
    as_qt_milestone_active_sts_cd,
    rev_planning_duration_days_cnt_in,
    as_milestone_start_dt_src,
    master_milestone_flg,
    ss_cd,
    TO_DATE(AS_MILESTONE_START_DT_SRC, 'yyyy-mm-dd') AS as_milestone_start_dt1,
    CURRENT_TIMESTAMP() AS created_dtm
    FROM source_ff_as_quote_milestone_hdp
),

final AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        as_quote_milestone_name,
        as_qt_milestone_te_mrgn_flg,
        as_qt_milestone_active_sts_cd,
        rev_planning_duration_days_cnt,
        as_milestone_start_dt,
        master_milestone_flg,
        ss_cd,
        created_dtm
    FROM transformed_exptrans
)

SELECT * FROM final
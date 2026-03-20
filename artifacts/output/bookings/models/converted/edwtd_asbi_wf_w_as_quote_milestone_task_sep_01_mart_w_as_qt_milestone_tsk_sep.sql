{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_quote_milestone_task_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_W_AS_QUOTE_MILESTONE_TASK_SEP',
        'target_table': 'W_AS_QT_MILESTONE_TSK_SEP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.944978+00:00'
    }
) }}

WITH 

source_w_as_qt_milestone_tsk_sep AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        as_qt_ms_task_list_usd_price,
        as_qt_ms_task_net_usd_price,
        as_qt_ms_task_expenses_usd_amt,
        as_qt_ms_task_usd_cost,
        as_qt_ms_task_type_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_as_qt_milestone_tsk_sep') }}
),

final AS (
    SELECT
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        as_qt_ms_task_list_usd_price,
        as_qt_ms_task_net_usd_price,
        as_qt_ms_task_expenses_usd_amt,
        as_qt_ms_task_usd_cost,
        as_qt_ms_task_type_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd,
        action_code,
        dml_type
    FROM source_w_as_qt_milestone_tsk_sep
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_quote_milestone_task_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QUOTE_MILESTONE_TASK_HDP',
        'target_table': 'ST_AS_QUOTE_MILESTONE_TASK_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.532077+00:00'
    }
) }}

WITH 

source_ff_as_quote_milestone_task_hdp AS (
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
        ss_cd
    FROM {{ source('raw', 'ff_as_quote_milestone_task_hdp') }}
),

transformed_exptrans AS (
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
    ss_cd,
    CURRENT_TIMESTAMP() AS created_dtm
    FROM source_ff_as_quote_milestone_task_hdp
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
        ss_cd,
        created_dtm
    FROM transformed_exptrans
)

SELECT * FROM final
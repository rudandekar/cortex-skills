{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_report_line_item_node_stg23nf', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_N_FIN_REPORT_LINE_ITEM_NODE_STG23NF',
        'target_table': 'N_FIN_REPORT_LINE_ITEM_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.713999+00:00'
    }
) }}

WITH 

source_st_n_financial_rli_pc_hier AS (
    SELECT
        parent_rli,
        child_rli,
        child_rli_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id,
        seq_no
    FROM {{ source('raw', 'st_n_financial_rli_pc_hier') }}
),

transformed_exptrans1 AS (
    SELECT
    bk_finance_report_line_item_cd,
    finance_report_line_item_descr,
    ru_prnt_fin_report_ln_item_cd,
    dv_level_num_int,
    line_item_node_type,
    rli_reporting_sequence_num_int
    FROM source_st_n_financial_rli_pc_hier
),

update_strategy_upd_ins AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exptrans1
    WHERE DD_INSERT != 3
),

update_strategy_upd_upd AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_ins
    WHERE DD_UPDATE != 3
),

transformed_exptrans AS (
    SELECT
    bk_finance_report_line_item_cd,
    finance_report_line_item_descr,
    ru_prnt_fin_report_ln_item_cd,
    dv_level_num_int,
    line_item_node_type,
    rli_reporting_sequence_num_int,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM update_strategy_upd_upd
),

final AS (
    SELECT
        bk_finance_report_line_item_cd,
        finance_report_line_item_descr,
        ru_prnt_fin_report_ln_item_cd,
        dv_level_num_int,
        line_item_node_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        rli_reporting_sequence_num_int
    FROM transformed_exptrans
)

SELECT * FROM final
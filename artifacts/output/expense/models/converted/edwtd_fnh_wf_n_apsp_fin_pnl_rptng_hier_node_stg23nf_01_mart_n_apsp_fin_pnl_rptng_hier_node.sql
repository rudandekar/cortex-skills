{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_apsp_fin_pnl_rptng_hier_node_stg23nf', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_N_APSP_FIN_PNL_RPTNG_HIER_NODE_STG23NF',
        'target_table': 'N_APSP_FIN_PNL_RPTNG_HIER_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.911569+00:00'
    }
) }}

WITH 

source_st_n_financial_pnl_pc_hier AS (
    SELECT
        batch_id,
        child_node,
        ifp_pnl_parent_lvl,
        mra_alias,
        parent_node,
        refresh_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_n_financial_pnl_pc_hier') }}
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
    FROM source_st_n_financial_pnl_pc_hier
    WHERE DD_INSERT != 3
),

transformed_exptrans1 AS (
    SELECT
    bk_pnl_rptng_hier_node_name,
    pnl_rptng_hier_node_descr,
    forecast_plng_level_num_int,
    hierarchy_node_type,
    dv_pnl_rptng_hier_node_lvl_int,
    source_deleted_flg,
    ru_prnt_pnl_rptng_hier_node_nm
    FROM update_strategy_upd_ins
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
    FROM transformed_exptrans1
    WHERE DD_UPDATE != 3
),

transformed_exptrans AS (
    SELECT
    bk_pnl_rptng_hier_node_name,
    pnl_rptng_hier_node_descr,
    forecast_plng_level_num_int,
    hierarchy_node_type,
    dv_pnl_rptng_hier_node_lvl_int,
    source_deleted_flg,
    ru_prnt_pnl_rptng_hier_node_nm,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM update_strategy_upd_upd
),

final AS (
    SELECT
        bk_pnl_rptng_hier_node_name,
        pnl_rptng_hier_node_descr,
        forecast_plng_level_num_int,
        hierarchy_node_type,
        dv_pnl_rptng_hier_node_lvl_int,
        source_deleted_flg,
        ru_prnt_pnl_rptng_hier_node_nm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM transformed_exptrans
)

SELECT * FROM final
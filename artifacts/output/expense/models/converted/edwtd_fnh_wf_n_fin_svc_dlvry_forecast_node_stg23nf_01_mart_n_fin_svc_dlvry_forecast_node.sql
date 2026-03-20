{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_svc_dlvry_forecast_node_stg23nf', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_N_FIN_SVC_DLVRY_FORECAST_NODE_STG23NF',
        'target_table': 'N_FIN_SVC_DLVRY_FORECAST_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.639763+00:00'
    }
) }}

WITH 

source_st_n_fin_fc_deliv_thtr_pc_hier AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id
    FROM {{ source('raw', 'st_n_fin_fc_deliv_thtr_pc_hier') }}
),

transformed_exp_st_n_fin_fc_deliv_thtr_pc_hier_upd AS (
    SELECT
    bk_fin_svc_dlvry_frcst_nd_nm,
    delivery_forecast_node_type,
    dv_level_num_int,
    ru_prt_fn_svc_dlvry_fcst_nd_nm,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM source_st_n_fin_fc_deliv_thtr_pc_hier
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
    FROM transformed_exp_st_n_fin_fc_deliv_thtr_pc_hier_upd
    WHERE DD_UPDATE != 3
),

transformed_exp_st_n_fin_fc_deliv_thtr_pc_hier_ins AS (
    SELECT
    bk_fin_svc_dlvry_frcst_nd_nm,
    delivery_forecast_node_type,
    dv_level_num_int,
    ru_prt_fn_svc_dlvry_fcst_nd_nm
    FROM update_strategy_upd_upd
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
    FROM transformed_exp_st_n_fin_fc_deliv_thtr_pc_hier_ins
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        bk_fin_svc_dlvry_frcst_nd_nm,
        delivery_forecast_node_type,
        dv_level_num_int,
        ru_prt_fn_svc_dlvry_fcst_nd_nm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_ins
)

SELECT * FROM final
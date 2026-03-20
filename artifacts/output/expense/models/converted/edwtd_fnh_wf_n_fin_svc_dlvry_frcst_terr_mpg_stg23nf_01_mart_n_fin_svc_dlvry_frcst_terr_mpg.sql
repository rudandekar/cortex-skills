{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_svc_dlvry_frcst_terr_mpg_stg23nf', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_N_FIN_SVC_DLVRY_FRCST_TERR_MPG_STG23NF',
        'target_table': 'N_FIN_SVC_DLVRY_FRCST_TERR_MPG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.894097+00:00'
    }
) }}

WITH 

source_st_n_fin_fc_deliv_theater_map AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id
    FROM {{ source('raw', 'st_n_fin_fc_deliv_theater_map') }}
),

transformed_exp_st_n_fin_fc_deliv_theater_map AS (
    SELECT
    bk_fin_svc_dlvry_frcst_nd_nm,
    sales_territory_key
    FROM source_st_n_fin_fc_deliv_theater_map
),

update_strategy_upd_ins AS (
    SELECT
        *,
        CASE 
            WHEN 0 = 0 THEN 'INSERT'
            WHEN 0 = 1 THEN 'UPDATE'
            WHEN 0 = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_st_n_fin_fc_deliv_theater_map
    WHERE 0 != 3
),

final AS (
    SELECT
        bk_fin_svc_dlvry_frcst_nd_nm,
        sales_territory_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_ins
)

SELECT * FROM final
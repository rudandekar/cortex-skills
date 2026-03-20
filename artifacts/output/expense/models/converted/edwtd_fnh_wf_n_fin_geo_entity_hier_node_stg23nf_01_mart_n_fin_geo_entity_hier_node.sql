{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_geo_entity_hier_node_stg23nf', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_N_FIN_GEO_ENTITY_HIER_NODE_STG23NF',
        'target_table': 'N_FIN_GEO_ENTITY_HIER_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.160479+00:00'
    }
) }}

WITH 

source_st_n_financial_entity_pc_hier AS (
    SELECT
        parent_entity,
        child_entity,
        child_entity_desc,
        child_entity_type,
        refresh_date,
        create_datetime,
        action_code,
        batch_id,
        seq_no
    FROM {{ source('raw', 'st_n_financial_entity_pc_hier') }}
),

transformed_exptrans1 AS (
    SELECT
    bk_fin_geo_enty_hier_node_name,
    dv_level_num_int,
    geographic_entity_node_type,
    ru_company_cd,
    ru_prnt_fin_geo_ent_hier_nd_nm,
    ge_reporting_sequence_num_int,
    geographic_entity_descr
    FROM source_st_n_financial_entity_pc_hier
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
    bk_fin_geo_enty_hier_node_name,
    dv_level_num_int,
    geographic_entity_node_type,
    ru_company_cd,
    ru_prnt_fin_geo_ent_hier_nd_nm,
    ge_reporting_sequence_num_int_in,
    geographic_entity_descr,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM update_strategy_upd_upd
),

final AS (
    SELECT
        bk_fin_geo_enty_hier_node_name,
        dv_level_num_int,
        geographic_entity_node_type,
        ru_company_cd,
        ru_prnt_fin_geo_ent_hier_nd_nm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ge_reporting_sequence_num_int,
        geographic_entity_descr
    FROM transformed_exptrans
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_finance_sales_theater_node_stg23nf', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_N_FINANCE_SALES_THEATER_NODE_STG23NF',
        'target_table': 'N_FINANCE_SALES_THEATER_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.052547+00:00'
    }
) }}

WITH 

source_st_n_financial_sales_pc_hier AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id
    FROM {{ source('raw', 'st_n_financial_sales_pc_hier') }}
),

transformed_exptrans AS (
    SELECT
    bk_fin_sales_theater_node_name,
    fin_sales_theater_node_descr,
    dv_level_num_int,
    sales_theater_node_type,
    ru_prnt_fin_sls_thtr_node_nm,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM source_st_n_financial_sales_pc_hier
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
    FROM transformed_exptrans
    WHERE DD_UPDATE != 3
),

transformed_exptrans1 AS (
    SELECT
    bk_fin_sales_theater_node_name,
    fin_sales_theater_node_descr,
    dv_level_num_int,
    sales_theater_node_type,
    ru_prnt_fin_sls_thtr_node_nm
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
    FROM transformed_exptrans1
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        bk_fin_sales_theater_node_name,
        fin_sales_theater_node_descr,
        dv_level_num_int,
        sales_theater_node_type,
        ru_prnt_fin_sls_thtr_node_nm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_ins
)

SELECT * FROM final
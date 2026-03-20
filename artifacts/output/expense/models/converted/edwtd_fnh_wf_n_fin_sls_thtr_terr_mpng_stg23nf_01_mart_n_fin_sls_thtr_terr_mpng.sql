{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_sls_thtr_terr_mpng_stg23nf', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_N_FIN_SLS_THTR_TERR_MPNG_STG23NF',
        'target_table': 'N_FIN_SLS_THTR_TERR_MPNG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.663943+00:00'
    }
) }}

WITH 

source_st_n_financial_sales_map AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id
    FROM {{ source('raw', 'st_n_financial_sales_map') }}
),

transformed_exptrans AS (
    SELECT
    bk_fin_sales_theater_node_name,
    sales_territory_key
    FROM source_st_n_financial_sales_map
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
    FROM transformed_exptrans
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        bk_fin_sales_theater_node_name,
        sales_territory_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_ins
)

SELECT * FROM final
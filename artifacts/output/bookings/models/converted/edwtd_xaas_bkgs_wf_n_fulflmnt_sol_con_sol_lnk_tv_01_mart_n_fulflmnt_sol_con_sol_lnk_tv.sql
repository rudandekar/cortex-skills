{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fulflmnt_sol_con_sol_lnk_tv', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_FULFLMNT_SOL_CON_SOL_LNK_TV',
        'target_table': 'N_FULFLMNT_SOL_CON_SOL_LNK_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.574172+00:00'
    }
) }}

WITH 

source_n_fulflmnt_sol_con_sol_lnk_tv AS (
    SELECT
        cons_sales_order_line_key,
        bk_web_order_id,
        fulflmnt_sales_order_line_key,
        start_tv_datetime,
        end_tv_datetime,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM {{ source('raw', 'n_fulflmnt_sol_con_sol_lnk_tv') }}
),

final AS (
    SELECT
        cons_sales_order_line_key,
        bk_web_order_id,
        fulflmnt_sales_order_line_key,
        start_tv_datetime,
        end_tv_datetime,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM source_n_fulflmnt_sol_con_sol_lnk_tv
)

SELECT * FROM final
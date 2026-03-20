{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_fulflmnt_sol_con_sol_lnk', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_W_FULFLMNT_SOL_CON_SOL_LNK',
        'target_table': 'W_FULFLMNT_SOL_CON_SOL_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.407968+00:00'
    }
) }}

WITH 

source_w_fulflmnt_sol_con_sol_lnk AS (
    SELECT
        cons_sales_order_line_key,
        bk_web_order_id,
        fulflmnt_sales_order_line_key,
        start_tv_datetime,
        end_tv_datetime,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fulflmnt_sol_con_sol_lnk') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_fulflmnt_sol_con_sol_lnk
)

SELECT * FROM final
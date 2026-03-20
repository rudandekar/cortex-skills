{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cg1_sales_order_line_dels', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_CG1_SALES_ORDER_LINE_DELS',
        'target_table': 'WI_CG1_SALES_ORDER_LINE_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.981577+00:00'
    }
) }}

WITH 

source_ex_cg1_oe_ord_ln_all_dels AS (
    SELECT
        line_id,
        action_code,
        error_msg,
        table_name,
        de_name,
        creation_date,
        last_update_date,
        source_commit_time,
        created_by,
        global_name,
        exception_type
    FROM {{ source('raw', 'ex_cg1_oe_ord_ln_all_dels') }}
),

source_st_cg1_oe_ord_ln_all_dels AS (
    SELECT
        line_id,
        action_code,
        error_msg,
        table_name,
        de_name,
        creation_date,
        last_update_date,
        source_commit_time,
        created_by,
        global_name
    FROM {{ source('raw', 'st_cg1_oe_ord_ln_all_dels') }}
),

final AS (
    SELECT
        line_id,
        creation_date,
        last_update_date,
        global_name
    FROM source_st_cg1_oe_ord_ln_all_dels
)

SELECT * FROM final
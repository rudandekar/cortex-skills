{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sol_order_line_status_date', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SOL_ORDER_LINE_STATUS_DATE',
        'target_table': 'WI_SOL_ORDER_LINE_STATUS_DATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.117092+00:00'
    }
) }}

WITH 

source_ex_cg1_xxgco_oe_ord_status_ln AS (
    SELECT
        header_id,
        line_id,
        status_result_code,
        status_result_date,
        created_program_name,
        program_id,
        request_id,
        org_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        source_commit_time,
        global_name,
        exception_type
    FROM {{ source('raw', 'ex_cg1_xxgco_oe_ord_status_ln') }}
),

source_st_cg1_xxgco_oe_ord_status_ln AS (
    SELECT
        header_id,
        line_id,
        status_result_code,
        status_result_date,
        created_program_name,
        program_id,
        request_id,
        org_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        source_commit_time,
        global_name
    FROM {{ source('raw', 'st_cg1_xxgco_oe_ord_status_ln') }}
),

final AS (
    SELECT
        sales_order_line_key,
        sk_so_line_id_int,
        status_result_code,
        status_result_date,
        creation_date,
        last_update_date,
        ss_code
    FROM source_st_cg1_xxgco_oe_ord_status_ln
)

SELECT * FROM final
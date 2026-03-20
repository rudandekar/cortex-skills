{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sol_end_customer_cg', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SOL_END_CUSTOMER_CG',
        'target_table': 'EX_CG1_OE_LINES_END_CUST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.739920+00:00'
    }
) }}

WITH 

source_st_cg1_oe_lines_end_cust AS (
    SELECT
        line_id,
        end_customer_site_use_id,
        end_customer_id,
        source_commit_time,
        global_name,
        end_customer_contact_id
    FROM {{ source('raw', 'st_cg1_oe_lines_end_cust') }}
),

source_st_cg_oe_headers_end_cust AS (
    SELECT
        header_id,
        end_customer_site_use_id,
        end_customer_id,
        source_commit_time,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_cg_oe_headers_end_cust') }}
),

source_ex_cg_oe_lines_end_cust AS (
    SELECT
        line_id,
        end_customer_site_use_id,
        end_customer_id,
        source_commit_time,
        action_code,
        create_datetime,
        exception_type,
        end_customer_contact_id
    FROM {{ source('raw', 'ex_cg_oe_lines_end_cust') }}
),

source_st_cg_oe_lines_end_cust AS (
    SELECT
        line_id,
        end_customer_site_use_id,
        end_customer_id,
        source_commit_time,
        action_code,
        create_datetime,
        end_customer_contact_id
    FROM {{ source('raw', 'st_cg_oe_lines_end_cust') }}
),

final AS (
    SELECT
        line_id,
        end_customer_site_use_id,
        end_customer_id,
        source_commit_time,
        global_name,
        exception_type,
        end_customer_contact_id
    FROM source_st_cg_oe_lines_end_cust
)

SELECT * FROM final
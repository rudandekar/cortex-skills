{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_so_order_status_date_dac', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SO_ORDER_STATUS_DATE_DAC',
        'target_table': 'WI_SO_ORDER_STATUS_DATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.999455+00:00'
    }
) }}

WITH 

source_ex_cg1_xxgco_oe_ord_status_hdr AS (
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
    FROM {{ source('raw', 'ex_cg1_xxgco_oe_ord_status_hdr') }}
),

source_st_cg1_xxgco_oe_ord_status_hdr AS (
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
    FROM {{ source('raw', 'st_cg1_xxgco_oe_ord_status_hdr') }}
),

final AS (
    SELECT
        sales_order_key,
        sk_sales_order_header_id_int,
        status_result_code,
        status_result_date,
        creation_date,
        last_update_date,
        ss_code
    FROM source_st_cg1_xxgco_oe_ord_status_hdr
)

SELECT * FROM final
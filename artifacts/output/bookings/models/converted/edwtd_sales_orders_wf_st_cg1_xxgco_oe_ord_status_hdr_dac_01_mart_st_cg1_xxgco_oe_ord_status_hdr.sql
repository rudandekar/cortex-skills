{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxgco_oe_ord_status_hdr_dac', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXGCO_OE_ORD_STATUS_HDR_DAC',
        'target_table': 'ST_CG1_XXGCO_OE_ORD_STATUS_HDR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.072946+00:00'
    }
) }}

WITH 

source_cg1_xxgco_oe_order_statuses AS (
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
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_xxgco_oe_order_statuses') }}
),

final AS (
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
    FROM source_cg1_xxgco_oe_order_statuses
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_oe_ord_lines_hist', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_OE_ORD_LINES_HIST',
        'target_table': 'ST_CG1_OE_ORD_LINES_HIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.995800+00:00'
    }
) }}

WITH 

source_cg1_oe_ord_lines_hist AS (
    SELECT
        line_id,
        ship_set_id,
        reason_code,
        line_number,
        hist_creation_date,
        hist_created_by,
        latest_cancelled_quantity,
        source_commit_time,
        trail_file_name,
        source_dml_type,
        refresh_datetime,
        processed_flag,
        cancelled_flag,
        cancelled_quantity,
        created_by,
        creation_date,
        header_id,
        hist_comments,
        hist_type_code,
        last_updated_by,
        last_update_date,
        line_category_code,
        line_set_id,
        line_type_id,
        link_to_line_id,
        ordered_quantity,
        request_date,
        request_id,
        return_reason_code,
        salesrep_id,
        shipment_number,
        org_id,
        reason_id
    FROM {{ source('raw', 'cg1_oe_ord_lines_hist') }}
),

final AS (
    SELECT
        line_id,
        ship_set_id,
        reason_code,
        line_number,
        hist_creation_date,
        hist_created_by,
        latest_cancelled_quantity,
        source_commit_time,
        global_name,
        cancelled_flag,
        cancelled_quantity,
        created_by,
        creation_date,
        header_id,
        hist_comments,
        hist_type_code,
        last_updated_by,
        last_update_date,
        line_category_code,
        line_set_id,
        line_type_id,
        link_to_line_id,
        ordered_quantity,
        request_date,
        request_id,
        return_reason_code,
        salesrep_id,
        shipment_number,
        org_id
    FROM source_cg1_oe_ord_lines_hist
)

SELECT * FROM final
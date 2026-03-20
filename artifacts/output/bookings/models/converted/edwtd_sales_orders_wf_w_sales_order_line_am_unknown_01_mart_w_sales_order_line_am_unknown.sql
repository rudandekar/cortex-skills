{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_line_am_unknown', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_LINE_AM_UNKNOWN',
        'target_table': 'W_SALES_ORDER_LINE_AM_UNKNOWN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.841663+00:00'
    }
) }}

WITH 

source_st_om_xxcca_oe_ord_lines_ex AS (
    SELECT
        line_id,
        global_name,
        attribute21,
        dispatch_date,
        dispatch_result_code,
        contract_quote_number,
        ges_update_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute12,
        receivable_intf_result_code,
        receivable_intf_date,
        batch_id,
        create_datetime,
        action_code,
        attribute20,
        attribute5,
        attribute1,
        attribute2,
        ship_to_po_number,
        delivery_option,
        attribute6,
        attribute4,
        taa,
        clean_config_flag,
        flexible_service_start_date,
        attribute25,
        attribute23,
        attribute14,
        attribute7
    FROM {{ source('raw', 'st_om_xxcca_oe_ord_lines_ex') }}
),

final AS (
    SELECT
        sales_order_key,
        sales_order_line_key,
        so_am_unknown_000_cd,
        so_am_unknown_001_cd,
        so_am_unknown_002_cd,
        so_am_unknown_003_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_om_xxcca_oe_ord_lines_ex
)

SELECT * FROM final
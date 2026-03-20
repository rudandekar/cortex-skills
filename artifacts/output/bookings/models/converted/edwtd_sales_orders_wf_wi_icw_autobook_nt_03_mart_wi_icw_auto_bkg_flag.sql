{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_icw_autobook_nt', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_ICW_AUTOBOOK_NT',
        'target_table': 'WI_ICW_AUTO_BKG_FLAG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.651178+00:00'
    }
) }}

WITH 

source_st_om_xxccp_oe_ord_hdr_ntflg AS (
    SELECT
        xxccp_header_id,
        quote_number,
        header_id,
        global_name,
        ges_update_date,
        status,
        sub_status,
        order_number,
        order_source_id,
        cs_case_number,
        cs_request_id,
        context,
        ordered_date,
        order_type_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        operation,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_xxccp_oe_ord_hdr_ntflg') }}
),

source_ex_om_xxccp_oe_ord_hdr_ntflg AS (
    SELECT
        xxccp_header_id,
        quote_number,
        header_id,
        global_name,
        ges_update_date,
        status,
        sub_status,
        order_number,
        order_source_id,
        cs_case_number,
        cs_request_id,
        context,
        ordered_date,
        order_type_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        operation,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_om_xxccp_oe_ord_hdr_ntflg') }}
),

source_st_om_xxccp_oe_order_header AS (
    SELECT
        xxccp_header_id,
        quote_number,
        header_id,
        global_name,
        ges_update_date,
        status,
        sub_status,
        order_number,
        order_source_id,
        cs_case_number,
        cs_request_id,
        context,
        ordered_date,
        order_type_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        operation,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_xxccp_oe_order_header') }}
),

final AS (
    SELECT
        xxccp_header_id,
        quote_number,
        sales_order_key,
        header_id,
        global_name,
        status,
        sub_status,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        operation
    FROM source_st_om_xxccp_oe_order_header
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rma_request_line_dmp', 'batch', 'edwtd_cs'],
    meta={
        'source_workflow': 'wf_m_W_RMA_REQUEST_LINE_DMP',
        'target_table': 'EX_DMP_CO_RETURN_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.121953+00:00'
    }
) }}

WITH 

source_wi_rma_request_line_dmp AS (
    SELECT
        bk_awaiting_authorization_num,
        dv_rma_request_submit_date,
        original_sales_order_key,
        rma_request_line_return_qty,
        line_id,
        creation_date
    FROM {{ source('raw', 'wi_rma_request_line_dmp') }}
),

source_st_dmp_co_return_line AS (
    SELECT
        batch_id,
        return_line_id,
        return_id,
        parent_return_line_id,
        part_number,
        description,
        line_number,
        unit_net_price,
        quantity,
        ext_net_price,
        ship_to_site_use_id,
        orig_erp_line_id,
        requested_ship_date,
        actual_ship_date,
        created_on,
        created_by,
        updated_on,
        updated_by,
        status,
        active,
        has_children,
        grand_parent_return_line_id,
        territory_id,
        share_node_id,
        item_type,
        unit_list_price,
        create_datetime,
        source_commit_time,
        refresh_datetime,
        action_code
    FROM {{ source('raw', 'st_dmp_co_return_line') }}
),

source_w_rma_request_line AS (
    SELECT
        bk_awaiting_authorization_num,
        dv_rma_request_submit_dt,
        original_sales_order_line_key,
        rma_request_line_return_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rma_request_line') }}
),

final AS (
    SELECT
        batch_id,
        return_line_id,
        return_id,
        parent_return_line_id,
        part_number,
        description,
        line_number,
        unit_net_price,
        quantity,
        ext_net_price,
        ship_to_site_use_id,
        orig_erp_line_id,
        requested_ship_date,
        actual_ship_date,
        created_on,
        created_by,
        updated_on,
        updated_by,
        status,
        active,
        has_children,
        grand_parent_return_line_id,
        territory_id,
        share_node_id,
        item_type,
        unit_list_price,
        create_datetime,
        source_commit_time,
        refresh_datetime,
        action_code,
        exception_type
    FROM source_w_rma_request_line
)

SELECT * FROM final
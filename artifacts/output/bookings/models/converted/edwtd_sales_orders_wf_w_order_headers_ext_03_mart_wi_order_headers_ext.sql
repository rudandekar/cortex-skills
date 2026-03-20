{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_order_headers_ext', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_ORDER_HEADERS_EXT',
        'target_table': 'WI_ORDER_HEADERS_EXT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.098534+00:00'
    }
) }}

WITH 

source_sm_sales_order AS (
    SELECT
        sales_order_key,
        bk_company_code,
        bk_set_of_books_key,
        bk_so_number_int,
        sk_sales_order_header_id_int,
        ss_code,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_sales_order') }}
),

source_st_uo_xxcca_oe_ord_headers_ext AS (
    SELECT
        header_id,
        attribute18,
        attribute19,
        attribute20,
        attribute3,
        attribute8,
        attribute_category,
        cisco_book_date,
        cisco_book_result_code,
        cms_source_header_id,
        cancel_order_date,
        complete_order_date,
        complete_order_result_code,
        program_type,
        expiration_date,
        doc_imaging_flag,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        batch_id,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_uo_xxcca_oe_ord_headers_ext') }}
),

source_st_bo_xxcca_oe_ord_headers_ext AS (
    SELECT
        batch_id,
        header_id,
        attribute18,
        attribute19,
        attribute20,
        attribute3,
        attribute8,
        attribute_category,
        cisco_book_date,
        cisco_book_result_code,
        cms_source_header_id,
        cancel_order_date,
        complete_order_date,
        complete_order_result_code,
        program_type,
        expiration_date,
        doc_imaging_flag,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_bo_xxcca_oe_ord_headers_ext') }}
),

final AS (
    SELECT
        sales_order_key,
        header_id,
        complete_order_result_code,
        sales_order_expiration_dtm,
        ru_cisco_booked_datetime,
        bk_deal_id,
        cisco_booked_status_role,
        edw_create_user,
        edw_create_datetime
    FROM source_st_bo_xxcca_oe_ord_headers_ext
)

SELECT * FROM final
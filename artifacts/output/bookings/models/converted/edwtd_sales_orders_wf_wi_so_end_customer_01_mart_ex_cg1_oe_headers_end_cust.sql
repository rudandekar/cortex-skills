{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_so_end_customer', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SO_END_CUSTOMER',
        'target_table': 'EX_CG1_OE_HEADERS_END_CUST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.630295+00:00'
    }
) }}

WITH 

source_ex_cg_oe_headers_end_cust AS (
    SELECT
        header_id,
        end_customer_site_use_id,
        end_customer_id,
        source_commit_time,
        action_code,
        create_datetime,
        exception_type
    FROM {{ source('raw', 'ex_cg_oe_headers_end_cust') }}
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

source_ex_bo_cca_so_end_csmrs_hdr AS (
    SELECT
        ges_pk_id,
        phone_id,
        postal_code,
        salesrep_id,
        ship_to_site_use_id,
        split_percentage,
        contact_id,
        country,
        data_source,
        end_customer_id,
        end_customer_version,
        header_id,
        line_id,
        market_segment,
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
        create_datetime,
        exception_type
    FROM {{ source('raw', 'ex_bo_cca_so_end_csmrs_hdr') }}
),

source_ex_uo_cca_so_end_csmrs_hdr AS (
    SELECT
        ges_pk_id,
        phone_id,
        postal_code,
        salesrep_id,
        ship_to_site_use_id,
        split_percentage,
        contact_id,
        country,
        data_source,
        end_customer_id,
        end_customer_version,
        header_id,
        line_id,
        market_segment,
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
        create_datetime,
        exception_type
    FROM {{ source('raw', 'ex_uo_cca_so_end_csmrs_hdr') }}
),

source_st_uo_cca_so_end_customers AS (
    SELECT
        ges_pk_id,
        phone_id,
        postal_code,
        salesrep_id,
        ship_to_site_use_id,
        split_percentage,
        contact_id,
        country,
        data_source,
        end_customer_id,
        end_customer_version,
        header_id,
        line_id,
        market_segment,
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
    FROM {{ source('raw', 'st_uo_cca_so_end_customers') }}
),

source_st_bo_cca_so_end_customers AS (
    SELECT
        ges_pk_id,
        phone_id,
        postal_code,
        salesrep_id,
        ship_to_site_use_id,
        split_percentage,
        contact_id,
        country,
        data_source,
        end_customer_id,
        end_customer_version,
        header_id,
        line_id,
        market_segment,
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
    FROM {{ source('raw', 'st_bo_cca_so_end_customers') }}
),

final AS (
    SELECT
        header_id,
        end_customer_site_use_id,
        end_customer_id,
        source_commit_time,
        global_name,
        exception_type
    FROM source_st_bo_cca_so_end_customers
)

SELECT * FROM final
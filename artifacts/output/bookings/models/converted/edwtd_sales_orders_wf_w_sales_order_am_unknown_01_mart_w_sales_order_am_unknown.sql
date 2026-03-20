{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_am_unknown', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_AM_UNKNOWN',
        'target_table': 'W_SALES_ORDER_AM_UNKNOWN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.105366+00:00'
    }
) }}

WITH 

source_st_om_xxcca_oe_ord_headers AS (
    SELECT
        header_id,
        global_name,
        attribute25,
        ot_header_discount,
        attribute3,
        folder_id,
        complete_order_date,
        ges_update_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute17,
        attribute4,
        attribute15,
        attribute7,
        attribute16,
        cancel_order_date,
        complete_order_result_code,
        attribute10,
        attribute6,
        cisco_book_result_code,
        attribute11,
        attribute18,
        attribute20,
        attribute5,
        action_code,
        batch_id,
        create_datetime,
        cbn_identifier
    FROM {{ source('raw', 'st_om_xxcca_oe_ord_headers') }}
),

final AS (
    SELECT
        sales_order_key,
        so_am_unknown_000_cd,
        so_am_unknown_001_cd,
        so_am_unknown_002_cd,
        so_am_unknown_003_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        so_am_unknown_004_cd
    FROM source_st_om_xxcca_oe_ord_headers
)

SELECT * FROM final
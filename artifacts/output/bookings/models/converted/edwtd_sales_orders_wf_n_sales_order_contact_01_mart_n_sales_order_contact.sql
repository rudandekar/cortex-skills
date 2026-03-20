{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_contact', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_CONTACT',
        'target_table': 'N_SALES_ORDER_CONTACT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.016183+00:00'
    }
) }}

WITH 

source_w_sales_order_contact AS (
    SELECT
        sales_order_contact_key,
        sales_order_key,
        contact_type_cd,
        contact_first_name,
        contact_last_name,
        contact_email_addr,
        source_deleted_flg,
        contact_telephone_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_contact') }}
),

final AS (
    SELECT
        sales_order_contact_key,
        sales_order_key,
        contact_type_cd,
        contact_first_name,
        contact_last_name,
        contact_email_addr,
        source_deleted_flg,
        contact_telephone_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_sales_order_contact
)

SELECT * FROM final
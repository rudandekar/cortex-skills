{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_contact', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_CONTACT',
        'target_table': 'W_SALES_ORDER_CONTACT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.071183+00:00'
    }
) }}

WITH 

source_st_cg1_xxgco_oe_ord_cont_exp AS (
    SELECT
        header_id,
        last_name,
        first_name,
        email_address,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        contact_type,
        telephone,
        exception_type,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_xxgco_oe_ord_cont_exp') }}
),

source_st_cg1_xxgco_oe_ord_cont AS (
    SELECT
        header_id,
        last_name,
        first_name,
        creation_date,
        created_by,
        contact_type
    FROM {{ source('raw', 'st_cg1_xxgco_oe_ord_cont') }}
),

source_sm_so_contact_person AS (
    SELECT
        sales_order_contact_key,
        contact_type_cd,
        contact_first_name,
        contact_last_name,
        sk_sales_order_header_id_int,
        ss_code,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_so_contact_person') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_sm_so_contact_person
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_contact', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_CONTACT',
        'target_table': 'EX_CG1_XXGCO_OE_ORD_CONT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.069811+00:00'
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
    FROM source_sm_so_contact_person
)

SELECT * FROM final
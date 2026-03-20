{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_disti_prmtn_apprvd_customer', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTI_PRMTN_APPRVD_CUSTOMER',
        'target_table': 'EX_DISTI_PRMTN_APPRVD_CUSTOMER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.096195+00:00'
    }
) }}

WITH 

source_st_dca_promo_customer_details AS (
    SELECT
        promo_id,
        promo_cust_detail_id,
        customer_id,
        customer_type,
        customer_name,
        customer_country_code,
        customer_address1,
        customer_address2,
        customer_city,
        customer_state,
        customer_zip_code,
        customer_phone,
        customer_account_no,
        customer_email_address,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        it_comments,
        batch_id,
        action_cd,
        create_datetime
    FROM {{ source('raw', 'st_dca_promo_customer_details') }}
),

transformed_exp_disti_prmtn_apprvd_customer AS (
    SELECT
    disti_prmtn_apprvd_cust_key,
    bk_promotion_num,
    bk_promotion_revision_num_int,
    bk_promotion_type_cd,
    customer_type,
    customer_name,
    customer_country_code,
    customer_address1,
    customer_address2,
    customer_city,
    customer_state,
    customer_zip_code,
    customer_phone,
    customer_email_address,
    promo_cust_detail_id,
    customer_account_no,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_dca_promo_customer_details
),

final AS (
    SELECT
        promo_id,
        promo_cust_detail_id,
        customer_id,
        customer_type,
        customer_name,
        customer_country_code,
        customer_address1,
        customer_address2,
        customer_city,
        customer_state,
        customer_zip_code,
        customer_phone,
        customer_account_no,
        customer_email_address,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        it_comments,
        batch_id,
        action_cd,
        create_datetime,
        exception_type
    FROM transformed_exp_disti_prmtn_apprvd_customer
)

SELECT * FROM final
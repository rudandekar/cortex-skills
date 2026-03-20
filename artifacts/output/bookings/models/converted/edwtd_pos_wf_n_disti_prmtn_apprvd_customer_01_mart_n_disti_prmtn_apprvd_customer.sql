{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_disti_prmtn_apprvd_customer', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTI_PRMTN_APPRVD_CUSTOMER',
        'target_table': 'N_DISTI_PRMTN_APPRVD_CUSTOMER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.377823+00:00'
    }
) }}

WITH 

source_w_disti_prmtn_apprvd_customer AS (
    SELECT
        disti_prmtn_apprvd_cust_key,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        disti_prmtn_customer_type_cd,
        reported_customer_name,
        reported_customer_country_cd,
        reported_customer_line_1_addr,
        reported_customer_line_2_addr,
        reported_customer_city_name,
        reported_customer_state_name,
        reported_customer_zip_cd,
        reported_customer_phone_num,
        reported_customer_email_addr,
        reported_customer_account_num,
        sk_promo_cust_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_disti_prmtn_apprvd_customer') }}
),

final AS (
    SELECT
        disti_prmtn_apprvd_cust_key,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        disti_prmtn_customer_type_cd,
        reported_customer_name,
        reported_customer_country_cd,
        reported_customer_line_1_addr,
        reported_customer_line_2_addr,
        reported_customer_city_name,
        reported_customer_state_name,
        reported_customer_zip_cd,
        reported_customer_phone_num,
        reported_customer_email_addr,
        reported_customer_account_num,
        sk_promo_cust_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_disti_prmtn_apprvd_customer
)

SELECT * FROM final
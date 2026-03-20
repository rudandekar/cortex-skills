{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dca_promo_customer_details', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_DCA_PROMO_CUSTOMER_DETAILS',
        'target_table': 'ST_DCA_PROMO_CUSTOMER_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.140864+00:00'
    }
) }}

WITH 

source_ff_dca_promo_customer_details AS (
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
    FROM {{ source('raw', 'ff_dca_promo_customer_details') }}
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
        create_datetime
    FROM source_ff_dca_promo_customer_details
)

SELECT * FROM final
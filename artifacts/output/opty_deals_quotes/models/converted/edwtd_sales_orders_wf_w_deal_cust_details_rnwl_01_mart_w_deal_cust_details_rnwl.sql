{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_cust_details_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_CUST_DETAILS_RNWL',
        'target_table': 'W_DEAL_CUST_DETAILS_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.978315+00:00'
    }
) }}

WITH 

source_w_deal_cust_details_rnwl AS (
    SELECT
        deal_cust_details_key,
        bk_deal_id,
        cust_registry_id,
        structured_acct_flg,
        cust_acct_name,
        line_1_addr,
        line_2_addr,
        line_3_addr,
        city_name,
        province_name,
        zip_cd,
        country_name,
        county_name,
        cust_type_cd,
        created_by_user_name,
        created_on_dtm,
        updated_by_user_name,
        updated_on_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_cust_details_rnwl') }}
),

final AS (
    SELECT
        deal_cust_details_key,
        bk_deal_id,
        cust_registry_id,
        structured_acct_flg,
        cust_acct_name,
        line_1_addr,
        line_2_addr,
        line_3_addr,
        city_name,
        province_name,
        zip_cd,
        country_name,
        county_name,
        cust_type_cd,
        created_by_user_name,
        created_on_dtm,
        updated_by_user_name,
        updated_on_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_deal_cust_details_rnwl
)

SELECT * FROM final
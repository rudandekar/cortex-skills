{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_dm_cust_rnwl_kfa', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_DM_CUST_RNWL_KFA',
        'target_table': 'ST_INT_RAW_DM_CUSTOMER_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.908691+00:00'
    }
) }}

WITH 

source_el_int_raw_dm_customer_rnwl_kafka AS (
    SELECT
        object_id,
        deal_object_id,
        cr_id,
        account_name,
        addr,
        addr_line_2,
        city,
        province,
        zipcode,
        country,
        county,
        country_code,
        edw_updated_date
    FROM {{ source('raw', 'el_int_raw_dm_customer_rnwl_kafka') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        opty_number,
        siebel_company_id,
        csc_comp_target_id,
        csc_site_id,
        csc_global_ultimate_id,
        account_type,
        structured_account,
        account_name,
        addr,
        addr_line_2,
        addr_line_3,
        city,
        province,
        zipcode,
        country,
        county,
        country_code,
        end_customer_type,
        created_by,
        created_on,
        updated_by,
        updated_on,
        cr_id,
        edw_updated_date
    FROM source_el_int_raw_dm_customer_rnwl_kafka
)

SELECT * FROM final
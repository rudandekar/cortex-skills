{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_revcloud_quote_headers', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_REVCLOUD_QUOTE_HEADERS',
        'target_table': 'EL_REVCLOUD_QUOTE_HEADERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.871754+00:00'
    }
) }}

WITH 

source_el_revcloud_quote_headers AS (
    SELECT
        created_by,
        created_on,
        fulfillement_type,
        intended_use_desc,
        deal_id,
        price_list_name,
        currency_code,
        quote_id,
        bill_to_site_id,
        quote_name,
        quote_status_desc,
        updated_by,
        updated_on,
        partner_be_geo_id,
        be_id,
        erp_price_list_id,
        cr_id,
        end_customer_name,
        address_line_1,
        city,
        zipcode,
        country,
        deal_status_desc,
        channel_acct_mngr,
        opty_id,
        federal_flag,
        quote_expiration_date,
        edw_create_dtm,
        topic_name,
        is_primary,
        edw_update_dtm
    FROM {{ source('raw', 'el_revcloud_quote_headers') }}
),

source_st_revcloud_quote_headers AS (
    SELECT
        created_by,
        created_on,
        fulfillement_type,
        intended_use_desc,
        deal_id,
        price_list_name,
        currency_code,
        quote_id,
        bill_to_site_id,
        quote_name,
        quote_status_desc,
        updated_by,
        updated_on,
        partner_be_geo_id,
        be_id,
        erp_price_list_id,
        cr_id,
        end_customer_name,
        address_line_1,
        city,
        zipcode,
        country,
        deal_status_desc,
        channel_acct_mngr,
        opty_id,
        federal_flag,
        quote_expiration_date,
        edw_create_dtm,
        record_offset,
        partition_number,
        topic_name,
        acct_mngr,
        role_name,
        is_primary
    FROM {{ source('raw', 'st_revcloud_quote_headers') }}
),

final AS (
    SELECT
        created_by,
        created_on,
        fulfillement_type,
        intended_use_desc,
        deal_id,
        price_list_name,
        currency_code,
        quote_id,
        bill_to_site_id,
        quote_name,
        quote_status_desc,
        updated_by,
        updated_on,
        partner_be_geo_id,
        be_id,
        erp_price_list_id,
        cr_id,
        end_customer_name,
        address_line_1,
        city,
        zipcode,
        country,
        deal_status_desc,
        channel_acct_mngr,
        opty_id,
        federal_flag,
        quote_expiration_date,
        edw_create_dtm,
        topic_name,
        is_primary,
        edw_update_dtm
    FROM source_st_revcloud_quote_headers
)

SELECT * FROM final
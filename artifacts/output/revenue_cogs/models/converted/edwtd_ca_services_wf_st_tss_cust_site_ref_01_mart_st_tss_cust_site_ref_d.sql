{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_cust_site_ref_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_CUST_SITE_REF_D',
        'target_table': 'ST_TSS_CUST_SITE_REF_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.235366+00:00'
    }
) }}

WITH 

source_ff_tss_cust_site_ref_d AS (
    SELECT
        customer_id,
        address_location_id,
        cust_market_seg_name,
        bl_last_update_date
    FROM {{ source('raw', 'ff_tss_cust_site_ref_d') }}
),

transformed_exptrans AS (
    SELECT
    customer_id,
    address_location_id,
    cust_market_seg_name,
    bl_last_update_date,
    IFF(LTRIM(RTRIM(CUSTOMER_ID)) = '\N',NULL,TO_INTEGER(CUSTOMER_ID)) AS o_customer_id,
    IFF(LTRIM(RTRIM(ADDRESS_LOCATION_ID)) = '\N',NULL,ADDRESS_LOCATION_ID) AS o_address_location_id,
    IFF(LTRIM(RTRIM(CUST_MARKET_SEG_NAME)) = '\N',NULL,CUST_MARKET_SEG_NAME) AS o_cust_market_seg_name,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date
    FROM source_ff_tss_cust_site_ref_d
),

final AS (
    SELECT
        customer_id,
        address_location_id,
        cust_market_seg_name,
        bl_last_update_date
    FROM transformed_exptrans
)

SELECT * FROM final
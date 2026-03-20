{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_cust_site_ref_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_CUST_SITE_REF_D',
        'target_table': 'EL_TSS_CUST_SITE_REF_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.255144+00:00'
    }
) }}

WITH 

source_st_tss_cust_site_ref_d AS (
    SELECT
        customer_id,
        address_location_id,
        cust_market_seg_name,
        bl_last_update_date
    FROM {{ source('raw', 'st_tss_cust_site_ref_d') }}
),

final AS (
    SELECT
        customer_id,
        address_location_id,
        cust_market_seg_name,
        bl_last_update_date,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_tss_cust_site_ref_d
)

SELECT * FROM final
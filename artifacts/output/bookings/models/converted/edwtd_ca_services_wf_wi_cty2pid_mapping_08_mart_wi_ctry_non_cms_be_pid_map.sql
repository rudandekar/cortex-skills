{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cty2pid_mapping', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_WI_CTY2PID_MAPPING',
        'target_table': 'WI_CTRY_Non_CMS_BE_PID_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.357179+00:00'
    }
) }}

WITH 

source_wi_ctry_cms_be_pf_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        business_entity_descr,
        bk_product_family_id,
        ctry_be_pf_alloc_pct,
        cum_pct
    FROM {{ source('raw', 'wi_ctry_cms_be_pf_map') }}
),

source_wi_ctry_sc_non_cms_be_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        business_entity_descr,
        iso_be_alloc_pct
    FROM {{ source('raw', 'wi_ctry_sc_non_cms_be_map') }}
),

source_wi_ctry_non_cms_be_pf_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        business_entity_descr,
        bk_product_family_id,
        ctry_be_pf_alloc_pct,
        cum_pct
    FROM {{ source('raw', 'wi_ctry_non_cms_be_pf_map') }}
),

source_wi_ctry_cms_be_pid_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        business_entity_descr,
        bk_product_family_id,
        bk_prdt_allctn_clsfctn_cd,
        bk_product_id,
        total_be,
        total_be_pf,
        tss_bookings_amount
    FROM {{ source('raw', 'wi_ctry_cms_be_pid_map') }}
),

source_wi_ctry_sc_cms_be_pid_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        business_entity_descr,
        goods_product_id,
        iso_sc_pct
    FROM {{ source('raw', 'wi_ctry_sc_cms_be_pid_map') }}
),

source_wi_ctry_non_cms_be_pid_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        business_entity_descr,
        bk_product_family_id,
        bk_prdt_allctn_clsfctn_cd,
        bk_product_id,
        total_be,
        total_be_pf,
        tss_bookings_amount
    FROM {{ source('raw', 'wi_ctry_non_cms_be_pid_map') }}
),

source_wi_ctry_sc_non_cms_be_pid_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        business_entity_descr,
        goods_product_id,
        iso_sc_pct
    FROM {{ source('raw', 'wi_ctry_sc_non_cms_be_pid_map') }}
),

source_wi_be_pf_adj_pid_map AS (
    SELECT
        business_entity_descr,
        bk_product_family_id,
        bk_prdt_allctn_clsfctn_cd,
        bk_product_id
    FROM {{ source('raw', 'wi_be_pf_adj_pid_map') }}
),

source_wi_ctry_sc_cms_be_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        business_entity_descr,
        iso_be_alloc_pct
    FROM {{ source('raw', 'wi_ctry_sc_cms_be_map') }}
),

final AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        business_entity_descr,
        bk_product_family_id,
        bk_prdt_allctn_clsfctn_cd,
        bk_product_id,
        total_be,
        total_be_pf,
        tss_bookings_amount
    FROM source_wi_ctry_sc_cms_be_map
)

SELECT * FROM final
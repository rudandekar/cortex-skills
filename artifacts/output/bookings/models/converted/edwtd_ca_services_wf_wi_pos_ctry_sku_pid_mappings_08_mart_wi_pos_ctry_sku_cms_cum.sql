{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pos_ctry_sku_pid_mappings', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_WI_POS_CTRY_SKU_PID_MAPPINGS',
        'target_table': 'WI_POS_CTRY_SKU_CMS_CUM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.232700+00:00'
    }
) }}

WITH 

source_wi_pos_ctry_non_cms_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        goods_product_id,
        iso_pct
    FROM {{ source('raw', 'wi_pos_ctry_non_cms_map') }}
),

source_wi_pos_sku_non_cms_map AS (
    SELECT
        service_sku,
        bk_service_category_id,
        goods_product_id,
        sku_pct
    FROM {{ source('raw', 'wi_pos_sku_non_cms_map') }}
),

source_wi_pos_sku_non_cms_cum AS (
    SELECT
        service_sku,
        bk_service_category_id,
        goods_product_id,
        total_sku,
        sku_pid_net,
        sku_pct,
        cum_pct
    FROM {{ source('raw', 'wi_pos_sku_non_cms_cum') }}
),

source_wi_pos_ctry_cms_cum AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        goods_product_id,
        total_iso,
        iso_pid_net,
        iso_pct,
        cum_pct
    FROM {{ source('raw', 'wi_pos_ctry_cms_cum') }}
),

source_wi_pos_ctry_non_cms_cum AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        goods_product_id,
        total_iso,
        iso_pid_net,
        iso_pct,
        cum_pct
    FROM {{ source('raw', 'wi_pos_ctry_non_cms_cum') }}
),

source_wi_pos_sku_cms_map AS (
    SELECT
        service_sku,
        bk_service_category_id,
        goods_product_id,
        sku_pct
    FROM {{ source('raw', 'wi_pos_sku_cms_map') }}
),

source_wi_pos_sku_cms_cum AS (
    SELECT
        service_sku,
        bk_service_category_id,
        goods_product_id,
        total_sku,
        sku_pid_net,
        sku_pct,
        cum_pct
    FROM {{ source('raw', 'wi_pos_sku_cms_cum') }}
),

source_wi_pos_ctry_cms_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        goods_product_id,
        iso_pct
    FROM {{ source('raw', 'wi_pos_ctry_cms_map') }}
),

source_wi_pos_ctry_sku_cms_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        service_sku,
        goods_product_id,
        iso_sku_pct
    FROM {{ source('raw', 'wi_pos_ctry_sku_cms_map') }}
),

source_wi_pos_ctry_sku_non_cms_map AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        service_sku,
        goods_product_id,
        iso_sku_pct
    FROM {{ source('raw', 'wi_pos_ctry_sku_non_cms_map') }}
),

source_wi_pos_ctry_sku_non_cms_cum AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        service_sku,
        goods_product_id,
        total_iso_sku,
        iso_sku_pid_net,
        iso_sku_pct,
        cum_pct
    FROM {{ source('raw', 'wi_pos_ctry_sku_non_cms_cum') }}
),

source_wi_pos_ctry_sku_cms_cum AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        service_sku,
        goods_product_id,
        total_iso_sku,
        iso_sku_pid_net,
        iso_sku_pct,
        cum_pct
    FROM {{ source('raw', 'wi_pos_ctry_sku_cms_cum') }}
),

final AS (
    SELECT
        iso_country_code,
        bk_service_category_id,
        service_sku,
        goods_product_id,
        total_iso_sku,
        iso_sku_pid_net,
        iso_sku_pct,
        cum_pct
    FROM source_wi_pos_ctry_sku_cms_cum
)

SELECT * FROM final
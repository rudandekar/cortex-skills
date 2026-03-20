{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_chnl_mtx_patterns_adj', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHNL_MTX_PATTERNS_ADJ',
        'target_table': 'WI_PROCESS_DATES_CHNL_MTX_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.093029+00:00'
    }
) }}

WITH 

source_wi_sales_chnl_mtx_patterns_adj AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sales_chnl_mtx_patterns_adj') }}
),

source_wi_process_dates_chnl_mtx_adj AS (
    SELECT
        bk_calendar_date
    FROM {{ source('raw', 'wi_process_dates_chnl_mtx_adj') }}
),

source_wi_chnl_pattern_bkg_sls_adj AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        process_date,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_chnl_pattern_bkg_sls_adj') }}
),

final AS (
    SELECT
        bk_calendar_date
    FROM source_wi_chnl_pattern_bkg_sls_adj
)

SELECT * FROM final
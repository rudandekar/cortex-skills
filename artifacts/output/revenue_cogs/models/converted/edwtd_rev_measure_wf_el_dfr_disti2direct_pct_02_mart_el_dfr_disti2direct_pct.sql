{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_dfr_disti2direct_pct', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_DFR_DISTI2DIRECT_PCT',
        'target_table': 'EL_DFR_DISTI2DIRECT_PCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.728569+00:00'
    }
) }}

WITH 

source_wi_defrev_ccw_month_cntl AS (
    SELECT
        processed_fiscal_year_mth_int
    FROM {{ source('raw', 'wi_defrev_ccw_month_cntl') }}
),

source_el_dfr_disti2direct_pct AS (
    SELECT
        fiscal_year_month_int,
        l3_sales_territory_name_code,
        sales_territory_name_code,
        sales_territory_key,
        allocation_pct,
        update_user,
        update_datetime,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        service_flg,
        driver_type,
        level1,
        level2
    FROM {{ source('raw', 'el_dfr_disti2direct_pct') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        l3_sales_territory_name_code,
        sales_territory_name_code,
        sales_territory_key,
        allocation_pct,
        update_user,
        update_datetime,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        service_flg,
        driver_type,
        level1,
        level2
    FROM source_el_dfr_disti2direct_pct
)

SELECT * FROM final
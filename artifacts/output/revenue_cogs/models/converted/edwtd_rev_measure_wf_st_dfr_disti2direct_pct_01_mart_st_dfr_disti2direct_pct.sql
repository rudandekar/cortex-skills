{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dfr_disti2direct_pct', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_DFR_DISTI2DIRECT_PCT',
        'target_table': 'ST_DFR_DISTI2DIRECT_PCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.547628+00:00'
    }
) }}

WITH 

source_ff_dfr_disti2direct_pct AS (
    SELECT
        processed_fiscal_month_id,
        disti_sl3,
        sales_territory_code,
        allocation_pct,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        service_flg,
        driver_type,
        level1,
        level2
    FROM {{ source('raw', 'ff_dfr_disti2direct_pct') }}
),

final AS (
    SELECT
        processed_fiscal_month_id,
        disti_sl3,
        sales_territory_code,
        allocation_pct,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        service_flg,
        driver_type,
        level1,
        level2
    FROM source_ff_dfr_disti2direct_pct
)

SELECT * FROM final
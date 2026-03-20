{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dfr_nonrec_rev_data', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_DFR_NONREC_REV_DATA',
        'target_table': 'ST_DFR_NONREC_REV_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.130778+00:00'
    }
) }}

WITH 

source_ff_dfr_nonrec_rev_data AS (
    SELECT
        processed_fiscal_month_id,
        fiscal_month_id,
        source_entity,
        measure_name,
        measure_level_2,
        service_flag,
        sales_territory_code,
        product_id,
        revenue_type,
        amount,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM {{ source('raw', 'ff_dfr_nonrec_rev_data') }}
),

final AS (
    SELECT
        processed_fiscal_month_id,
        fiscal_month_id,
        source_entity,
        measure_name,
        measure_level_2,
        service_flag,
        sales_territory_code,
        product_id,
        revenue_type,
        amount,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM source_ff_dfr_nonrec_rev_data
)

SELECT * FROM final
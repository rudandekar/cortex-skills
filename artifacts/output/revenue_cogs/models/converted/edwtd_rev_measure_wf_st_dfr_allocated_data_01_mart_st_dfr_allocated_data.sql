{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dfr_allocated_data', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_DFR_ALLOCATED_DATA',
        'target_table': 'ST_DFR_ALLOCATED_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.347681+00:00'
    }
) }}

WITH 

source_ff_dfr_allocated_data AS (
    SELECT
        processed_fiscal_month_id,
        source_entity,
        measure_name,
        sales_territory_code,
        product_family_id,
        product_id,
        deal_id,
        profile_id,
        fiscal_month_id,
        rev_type,
        service_flg,
        amount,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM {{ source('raw', 'ff_dfr_allocated_data') }}
),

final AS (
    SELECT
        processed_fiscal_month_id,
        source_entity,
        measure_name,
        sales_territory_code,
        product_family_id,
        product_id,
        deal_id,
        profile_id,
        fiscal_month_id,
        rev_type,
        service_flg,
        amount,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM source_ff_dfr_allocated_data
)

SELECT * FROM final
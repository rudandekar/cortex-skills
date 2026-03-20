{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_dfr_nonrec_rev_data', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_DFR_NONREC_REV_DATA',
        'target_table': 'EL_DFR_NONREC_REV_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.712238+00:00'
    }
) }}

WITH 

source_el_dfr_nonrec_rev_data AS (
    SELECT
        processed_fiscal_month_id,
        measure_name,
        measure_level_2,
        service_flag,
        sales_territory_code,
        product_key,
        revenue_type,
        amount,
        src_create_user,
        src_create_datetime,
        src_update_user,
        src_update_datetime,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        fiscal_month_id,
        source_entity
    FROM {{ source('raw', 'el_dfr_nonrec_rev_data') }}
),

final AS (
    SELECT
        processed_fiscal_month_id,
        measure_name,
        measure_level_2,
        service_flag,
        sales_territory_code,
        product_key,
        revenue_type,
        amount,
        src_create_user,
        src_create_datetime,
        src_update_user,
        src_update_datetime,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        fiscal_month_id,
        source_entity
    FROM source_el_dfr_nonrec_rev_data
)

SELECT * FROM final
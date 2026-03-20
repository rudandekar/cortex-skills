{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_dfr_gross_unbilled_avg_term', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_DFR_GROSS_UNBILLED_AVG_TERM',
        'target_table': 'EL_DFR_GROSS_UNBILLED_AVG_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.741562+00:00'
    }
) }}

WITH 

source_el_dfr_gross_unbilled_avg_term AS (
    SELECT
        business_unit,
        effective_start_date,
        end_date,
        avg_term,
        created_by,
        create_date,
        updated_by,
        update_date
    FROM {{ source('raw', 'el_dfr_gross_unbilled_avg_term') }}
),

final AS (
    SELECT
        business_unit,
        effective_start_date,
        end_date,
        avg_term,
        created_by,
        create_date,
        updated_by,
        update_date
    FROM source_el_dfr_gross_unbilled_avg_term
)

SELECT * FROM final
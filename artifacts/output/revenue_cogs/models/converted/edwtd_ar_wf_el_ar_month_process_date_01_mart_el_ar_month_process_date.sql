{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ar_month_process_date', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_AR_MONTH_PROCESS_DATE',
        'target_table': 'EL_AR_MONTH_PROCESS_DATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.886748+00:00'
    }
) }}

WITH 

source_el_ar_month_process_date AS (
    SELECT
        fiscal_id,
        start_date,
        end_date,
        sc_last_process_date
    FROM {{ source('raw', 'el_ar_month_process_date') }}
),

final AS (
    SELECT
        fiscal_id,
        start_date,
        end_date,
        sc_last_process_date
    FROM source_el_ar_month_process_date
)

SELECT * FROM final
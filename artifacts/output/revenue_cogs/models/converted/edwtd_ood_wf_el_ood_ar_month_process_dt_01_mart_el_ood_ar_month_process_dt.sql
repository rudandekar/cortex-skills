{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ood_ar_month_process_dt', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_EL_OOD_AR_MONTH_PROCESS_DT',
        'target_table': 'EL_OOD_AR_MONTH_PROCESS_DT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.028272+00:00'
    }
) }}

WITH 

source_el_ood_ar_month_process_dt AS (
    SELECT
        fiscal_id,
        start_date,
        end_date,
        sc_last_process_date
    FROM {{ source('raw', 'el_ood_ar_month_process_dt') }}
),

final AS (
    SELECT
        fiscal_id,
        start_date,
        end_date,
        sc_last_process_date
    FROM source_el_ood_ar_month_process_dt
)

SELECT * FROM final
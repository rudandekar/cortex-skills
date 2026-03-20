{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_qtr_proj_mnth_cntl_src', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_QTR_PROJ_MNTH_CNTL_SRC',
        'target_table': 'WI_QTR_PROJ_MNTH_CNTL_SRC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.517851+00:00'
    }
) }}

WITH 

source_wi_qtr_proj_mnth_cntl_src AS (
    SELECT
        fiscal_year_month_int,
        run_batch_id
    FROM {{ source('raw', 'wi_qtr_proj_mnth_cntl_src') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        run_batch_id
    FROM source_wi_qtr_proj_mnth_cntl_src
)

SELECT * FROM final
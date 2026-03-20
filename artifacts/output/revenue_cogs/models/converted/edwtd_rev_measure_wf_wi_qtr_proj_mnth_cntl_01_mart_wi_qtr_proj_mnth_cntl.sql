{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_qtr_proj_mnth_cntl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_QTR_PROJ_MNTH_CNTL',
        'target_table': 'WI_QTR_PROJ_MNTH_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.110784+00:00'
    }
) }}

WITH 

source_wi_qtr_proj_mnth_cntl AS (
    SELECT
        processed_fiscal_year_qtr_int,
        processed_fiscal_year_mth_int,
        fiscal_year_month_int,
        run_batch_id,
        inc_flag
    FROM {{ source('raw', 'wi_qtr_proj_mnth_cntl') }}
),

final AS (
    SELECT
        processed_fiscal_year_qtr_int,
        processed_fiscal_year_mth_int,
        fiscal_year_month_int,
        run_batch_id,
        inc_flag
    FROM source_wi_qtr_proj_mnth_cntl
)

SELECT * FROM final
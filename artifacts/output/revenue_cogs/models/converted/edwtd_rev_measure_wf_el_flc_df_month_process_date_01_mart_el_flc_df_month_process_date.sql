{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_flc_df_month_process_date', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_FLC_DF_MONTH_PROCESS_DATE',
        'target_table': 'EL_FLC_DF_MONTH_PROCESS_DATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.252852+00:00'
    }
) }}

WITH 

source_el_flc_df_month_process_date AS (
    SELECT
        processed_fiscal_year_mth_int,
        start_date,
        fiscal_month_end_date,
        dv_ar_trx_dtm
    FROM {{ source('raw', 'el_flc_df_month_process_date') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        start_date,
        fiscal_month_end_date,
        dv_ar_trx_dtm
    FROM source_el_flc_df_month_process_date
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rs_ctrl_mth', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_RS_CTRL_MTH',
        'target_table': 'WI_RS_CTRL_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.941448+00:00'
    }
) }}

WITH 

source_wi_rs_ctrl_mth AS (
    SELECT
        fiscal_year_month_int,
        process_flag,
        current_fiscal_month_flag,
        fiscal_year_quarter_number_int,
        current_fiscal_qtr_flag
    FROM {{ source('raw', 'wi_rs_ctrl_mth') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        process_flag,
        current_fiscal_month_flag,
        fiscal_year_quarter_number_int,
        current_fiscal_qtr_flag
    FROM source_wi_rs_ctrl_mth
)

SELECT * FROM final
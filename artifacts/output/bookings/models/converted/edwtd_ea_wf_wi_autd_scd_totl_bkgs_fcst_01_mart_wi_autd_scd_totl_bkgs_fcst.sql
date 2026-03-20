{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_autd_scd_totl_bkgs_fcst', 'batch', 'edwtd_ea'],
    meta={
        'source_workflow': 'wf_m_WI_AUTD_SCD_TOTL_BKGS_FCST',
        'target_table': 'WI_AUTD_SCD_TOTL_BKGS_FCST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.274518+00:00'
    }
) }}

WITH 

source_wi_autd_scd_totl_bkgs_fcst AS (
    SELECT
        scorecard_id,
        metric_id,
        measure_name,
        hierarchy_type,
        amount_type_desc,
        time_granularity_desc,
        fiscal_quarter_name,
        fiscal_quarter_id,
        hierarchy_value,
        acquisition_source_grp,
        amount_value_fmt,
        amount_value,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'wi_autd_scd_totl_bkgs_fcst') }}
),

final AS (
    SELECT
        scorecard_id,
        metric_id,
        measure_name,
        hierarchy_type,
        amount_type_desc,
        time_granularity_desc,
        fiscal_quarter_name,
        fiscal_quarter_id,
        hierarchy_value,
        acquisition_source_grp,
        amount_value_fmt,
        amount_value,
        edw_create_dtm,
        edw_create_user
    FROM source_wi_autd_scd_totl_bkgs_fcst
)

SELECT * FROM final
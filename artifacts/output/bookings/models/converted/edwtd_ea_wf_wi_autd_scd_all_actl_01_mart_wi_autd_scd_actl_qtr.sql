{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_autd_scd_all_actl', 'batch', 'edwtd_ea'],
    meta={
        'source_workflow': 'wf_m_WI_AUTD_SCD_ALL_ACTL',
        'target_table': 'WI_AUTD_SCD_ACTL_QTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.312484+00:00'
    }
) }}

WITH 

source_wi_autd_scd_actl_qtr AS (
    SELECT
        scorecard_id,
        metric_id,
        measure_name_dim1,
        hierarchy_type,
        amount_type_desc,
        time_granularity_desc,
        fiscal_quarter_name,
        fiscal_quarter_id,
        acquisition_source_grp_dim2,
        hierarchy_value_dim3,
        hierarchy_value_dim4,
        hierarchy_value_dim5,
        amount_value,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'wi_autd_scd_actl_qtr') }}
),

final AS (
    SELECT
        scorecard_id,
        metric_id,
        measure_name_dim1,
        hierarchy_type,
        amount_type_desc,
        time_granularity_desc,
        fiscal_quarter_name,
        fiscal_quarter_id,
        acquisition_source_grp_dim2,
        hierarchy_value_dim3,
        hierarchy_value_dim4,
        hierarchy_value_dim5,
        amount_value,
        edw_create_dtm,
        edw_create_user
    FROM source_wi_autd_scd_actl_qtr
)

SELECT * FROM final
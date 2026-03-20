{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_wi_autd_scd_y2y_data_sbe', 'batch', 'edwtd_ea'],
    meta={
        'source_workflow': 'wf_m_WI_AUTD_SCD_Y2Y_DATA_SBE',
        'target_table': 'WI_AUTD_SCD_GMRGN_REV_INTRM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.459250+00:00'
    }
) }}

WITH 

source_wi_autd_scd_y2y_actl_mth AS (
    SELECT
        scorecard_id,
        metric_id,
        measure_name_dim1,
        hierarchy_type,
        amount_type_desc,
        time_granularity_desc,
        fiscal_quarter_name,
        fiscal_month_name,
        fiscal_year_month_int,
        fiscal_quarter_id,
        acquisition_source_grp_dim2,
        hierarchy_value_dim3,
        hierarchy_value_dim4,
        hierarchy_value_dim5,
        amount_value,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'wi_autd_scd_y2y_actl_mth') }}
),

source_wi_autd_scd_y2y_data AS (
    SELECT
        scorecard_id,
        metric_id,
        metric_class,
        measure_name_dim1,
        hierarchy_type,
        amount_type_desc,
        time_granularity_desc,
        fiscal_quarter_id,
        prev_yr_fiscal_quarter_id,
        fiscal_quarter_name,
        prev_yr_fiscal_quarter_name,
        hierarchy_value_dim3,
        hierarchy_value_dim4,
        hierarchy_value_dim5,
        acquisition_source_grp_dim2,
        actual_amount,
        prev_yr_actual_amount,
        y2y_trend_value,
        y2y_trend_value_fmt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'wi_autd_scd_y2y_data') }}
),

source_wi_autd_scd_gmrgn_rev_intrm AS (
    SELECT
        scorecard_id,
        metric_id,
        measure_name_dim1,
        hierarchy_type,
        amount_type_desc,
        time_granularity_desc,
        fiscal_quarter_name,
        fiscal_month_name,
        fiscal_year_month_int,
        fiscal_quarter_id,
        acquisition_source_grp_dim2,
        hierarchy_value_dim3,
        hierarchy_value_dim4,
        hierarchy_value_dim5,
        amount_value,
        net_rev_amt,
        net_cost_amt,
        gross_margin,
        revenue_margin,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'wi_autd_scd_gmrgn_rev_intrm') }}
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
        fiscal_month_name,
        fiscal_year_month_int,
        fiscal_quarter_id,
        acquisition_source_grp_dim2,
        hierarchy_value_dim3,
        hierarchy_value_dim4,
        hierarchy_value_dim5,
        amount_value,
        net_rev_amt,
        net_cost_amt,
        gross_margin,
        revenue_margin,
        edw_create_dtm,
        edw_create_user
    FROM source_wi_autd_scd_gmrgn_rev_intrm
)

SELECT * FROM final
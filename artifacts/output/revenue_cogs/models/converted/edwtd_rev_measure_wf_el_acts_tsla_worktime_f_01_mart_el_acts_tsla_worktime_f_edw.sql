{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_acts_tsla_worktime_f', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_ACTS_TSLA_WORKTIME_F',
        'target_table': 'EL_ACTS_TSLA_WORKTIME_F_EDW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.678557+00:00'
    }
) }}

WITH 

source_el_acts_tsla_worktime_f_edw AS (
    SELECT
        fiscal_year_month_int,
        fiscal_month_name,
        fiscal_year_id,
        incident_number,
        incident_status_id,
        incident_status,
        bl_incident_key,
        dw_case_activity_key,
        chg_dw_workgroup_key,
        chg_wkgrp_name,
        chg_wkgrp_theater_name,
        chg_wkgrp_delivery_channel,
        activity_code,
        activity_date,
        acts_activity_name,
        acts_tsla,
        acts_consistency_constant,
        acts_median_tsla,
        acts_abs_deviation_tsla,
        acts_median_abs_deviation_tsla,
        acts_median_abs_dev_threshold,
        acts_outlier_flag,
        acts_mean_non_outlier_tsla,
        acts_worktime,
        dw_load_date,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        fiscal_month_id
    FROM {{ source('raw', 'el_acts_tsla_worktime_f_edw') }}
),

source_el_acts_tsla_worktime_f AS (
    SELECT
        fiscal_year_month_int,
        fiscal_month_name,
        fiscal_year_id,
        incident_number,
        incident_status_id,
        incident_status,
        bl_incident_key,
        dw_case_activity_key,
        chg_dw_workgroup_key,
        chg_wkgrp_name,
        chg_wkgrp_theater_name,
        chg_wkgrp_delivery_channel,
        activity_code,
        activity_date,
        acts_activity_name,
        acts_tsla,
        acts_consistency_constant,
        acts_median_tsla,
        acts_abs_deviation_tsla,
        acts_median_abs_deviation_tsla,
        acts_median_abs_dev_threshold,
        acts_outlier_flag,
        acts_mean_non_outlier_tsla,
        acts_worktime,
        dw_load_date,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        fiscal_month_id
    FROM {{ source('raw', 'el_acts_tsla_worktime_f') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        fiscal_month_name,
        fiscal_year_id,
        incident_number,
        incident_status_id,
        incident_status,
        bl_incident_key,
        dw_case_activity_key,
        chg_dw_workgroup_key,
        chg_wkgrp_name,
        chg_wkgrp_theater_name,
        chg_wkgrp_delivery_channel,
        activity_code,
        activity_date,
        acts_activity_name,
        acts_tsla,
        acts_consistency_constant,
        acts_median_tsla,
        acts_abs_deviation_tsla,
        acts_median_abs_deviation_tsla,
        acts_median_abs_dev_threshold,
        acts_outlier_flag,
        acts_mean_non_outlier_tsla,
        acts_worktime,
        dw_load_date,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        fiscal_month_id
    FROM source_el_acts_tsla_worktime_f
)

SELECT * FROM final
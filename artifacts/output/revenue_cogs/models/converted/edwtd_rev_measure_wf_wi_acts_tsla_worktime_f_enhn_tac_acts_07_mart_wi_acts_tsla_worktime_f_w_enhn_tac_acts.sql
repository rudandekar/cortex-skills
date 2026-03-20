{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_acts_tsla_worktime_f_enhn_tac_acts', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_ACTS_TSLA_WORKTIME_F_ENHN_TAC_ACTS',
        'target_table': 'WI_ACTS_TSLA_WORKTIME_F_W_ENHN_TAC_ACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.294938+00:00'
    }
) }}

WITH 

source_wi_acts_tsla_worktime_f_enhn_tac_acts AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        fiscal_month_status_ind,
        acts_measures_calc_date,
        fiscal_year_id,
        incident_id,
        incident_number,
        incident_status_id,
        incident_status,
        bl_incident_key,
        dw_case_activity_key,
        dw_crnt_keyword_key,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wkgrp_natural_key,
        chg_wkgrp_name,
        chg_wkgrp_tac_center_name,
        chg_wkgrp_theater_name,
        chg_wkgrp_master_theater,
        chg_wkgrp_delivery_channel,
        activity_name,
        activity_addnl_info,
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
        acts_worktime
    FROM {{ source('raw', 'wi_acts_tsla_worktime_f_enhn_tac_acts') }}
),

source_wi_worktime_f_0_consol_enhn_tac_acts AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        fiscal_month_status_ind,
        acts_measures_calc_date,
        fiscal_year_id,
        incident_id,
        incident_number,
        incident_status_id,
        incident_status,
        dw_case_activity_key,
        dw_crnt_keyword_key,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wkgrp_natural_key,
        chg_wkgrp_master_theater,
        activity_name,
        activity_addnl_info,
        activity_code,
        activity_code_sort,
        activity_date,
        activity_date_override,
        acts_activity_name,
        acts_include_flag,
        etl_phase_ind
    FROM {{ source('raw', 'wi_worktime_f_0_consol_enhn_tac_acts') }}
),

source_wi_worktime_f_0_init_enhn_tac_acts AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        fiscal_month_status_ind,
        acts_measures_calc_date,
        fiscal_year_id,
        incident_id,
        incident_number,
        incident_status_id,
        incident_status,
        dw_case_activity_key,
        dw_crnt_keyword_key,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wkgrp_natural_key,
        chg_wkgrp_master_theater,
        activity_name,
        activity_addnl_info,
        activity_code,
        activity_code_sort,
        activity_date,
        acts_activity_name,
        acts_include_flag,
        etl_phase_ind
    FROM {{ source('raw', 'wi_worktime_f_0_init_enhn_tac_acts') }}
),

source_wi_acts_tsla_worktime_f_w_enhn_tac_acts AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        dw_workgroup_key
    FROM {{ source('raw', 'wi_acts_tsla_worktime_f_w_enhn_tac_acts') }}
),

source_wi_worktime_f_1_enhn_tac_acts AS (
    SELECT
        chg_cco_id,
        incident_id,
        incident_number,
        incident_status_id,
        consolidate_start_date,
        fiscal_month_name,
        consolidate_end_date
    FROM {{ source('raw', 'wi_worktime_f_1_enhn_tac_acts') }}
),

source_wi_worktime_f_0_init_0_enhn_tac_acts AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        fiscal_month_status_ind,
        acts_measures_calc_date,
        fiscal_year_id,
        incident_id,
        incident_number,
        incident_status_id,
        incident_status,
        dw_case_activity_key,
        dw_crnt_keyword_key,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wkgrp_natural_key,
        chg_wkgrp_master_theater,
        activity_name,
        activity_addnl_info,
        activity_code,
        activity_code_sort,
        activity_date,
        acts_activity_name,
        acts_include_flag,
        etl_phase_ind
    FROM {{ source('raw', 'wi_worktime_f_0_init_0_enhn_tac_acts') }}
),

source_wi_worktime_f_0_mtsla_enhn_tac_acts AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        fiscal_month_status_ind,
        acts_measures_calc_date,
        fiscal_year_id,
        incident_id,
        incident_number,
        incident_status_id,
        incident_status,
        dw_case_activity_key,
        dw_crnt_keyword_key,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wkgrp_natural_key,
        chg_wkgrp_master_theater,
        activity_name,
        activity_addnl_info,
        activity_code,
        activity_code_sort,
        activity_date,
        activity_date_override,
        acts_activity_name,
        acts_include_flag,
        acts_tsla,
        acts_consistency_constant,
        acts_median_tsla,
        acts_abs_deviation_tsla,
        etl_phase_ind
    FROM {{ source('raw', 'wi_worktime_f_0_mtsla_enhn_tac_acts') }}
),

source_wi_worktime_f_0_tsla_enhn_tac_acts AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        fiscal_month_status_ind,
        acts_measures_calc_date,
        fiscal_year_id,
        incident_id,
        incident_number,
        incident_status_id,
        incident_status,
        dw_case_activity_key,
        dw_crnt_keyword_key,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wkgrp_natural_key,
        chg_wkgrp_master_theater,
        activity_name,
        activity_addnl_info,
        activity_code,
        activity_code_sort,
        activity_date,
        activity_date_override,
        acts_activity_name,
        time_difference,
        acts_include_flag,
        acts_tsla,
        etl_phase_ind
    FROM {{ source('raw', 'wi_worktime_f_0_tsla_enhn_tac_acts') }}
),

source_wi_worktime_f_3_amnotsla_enhn_tac_acts AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        fiscal_month_status_ind,
        acts_measures_calc_date,
        acts_measures_start_date,
        acts_measures_end_date,
        acts_activity_name,
        acts_median_tsla,
        acts_median_abs_deviation_tsla,
        acts_mean_non_outlier_tsla
    FROM {{ source('raw', 'wi_worktime_f_3_amnotsla_enhn_tac_acts') }}
),

source_wi_worktime_f_0_madtsla_enhn_tac_acts AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        fiscal_month_status_ind,
        acts_measures_calc_date,
        fiscal_year_id,
        incident_id,
        incident_number,
        incident_status_id,
        incident_status,
        dw_case_activity_key,
        dw_crnt_keyword_key,
        chg_cco_id,
        chg_dw_workgroup_key,
        chg_wkgrp_natural_key,
        chg_wkgrp_master_theater,
        activity_name,
        activity_addnl_info,
        activity_code,
        activity_code_sort,
        activity_date,
        activity_date_override,
        acts_activity_name,
        acts_include_flag,
        acts_tsla,
        acts_consistency_constant,
        acts_median_tsla,
        acts_abs_deviation_tsla,
        acts_median_abs_deviation_tsla,
        acts_median_abs_dev_threshold,
        acts_outlier_flag,
        etl_phase_ind
    FROM {{ source('raw', 'wi_worktime_f_0_madtsla_enhn_tac_acts') }}
),

final AS (
    SELECT
        fiscal_month_id,
        fiscal_month_name,
        dw_workgroup_key
    FROM source_wi_worktime_f_0_madtsla_enhn_tac_acts
)

SELECT * FROM final
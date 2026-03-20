{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sr_activity_acts_enhn_tac_acts', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_SR_ACTIVITY_ACTS_ENHN_TAC_ACTS',
        'target_table': 'N_ACTIVITY_TAC_ACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.190225+00:00'
    }
) }}

WITH 

source_n_activity_tac_acts AS (
    SELECT
        sr_activity_acts_key,
        bk_fiscal_calendar_cd,
        bk_fiscal_month_num_int,
        bk_fiscal_year_num_int,
        activity_code_int,
        activity_additional_info_txt,
        activity_dtm,
        dv_activity_dt,
        activity_name,
        acts_abslt_dvn_tsla_mins_cnt,
        acts_activity_name,
        acts_consistency_constant_amt,
        acts_mean_non_outlier_tsla_amt,
        acts_measure_calc_dt,
        acts_mdn_abslt_dvn_thrshld_amt,
        acts_mdn_abslt_dvn_tsla_amt,
        acts_outlier_flg,
        acts_mdn_tsla_amt,
        tsla_mins_cnt,
        worktime_adjusted_for_acts_cnt,
        current_keyword_id,
        fiscal_month_closed_status_cd,
        service_request_status_name,
        service_request_status_id_int,
        change_csco_wrkr_prty_key,
        change_delivery_channel_name,
        change_tac_name,
        change_wg_master_theater_name,
        sk_change_dw_workgroup_key,
        change_workgroup_name,
        sk_change_wg_natural_key,
        change_workgroup_theater_name,
        dw_case_activity_key_id,
        bk_service_request_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_activity_tac_acts') }}
),

final AS (
    SELECT
        sr_activity_acts_key,
        bk_fiscal_calendar_cd,
        bk_fiscal_month_num_int,
        bk_fiscal_year_num_int,
        activity_code_int,
        activity_additional_info_txt,
        activity_dtm,
        dv_activity_dt,
        activity_name,
        acts_abslt_dvn_tsla_mins_cnt,
        acts_activity_name,
        acts_consistency_constant_amt,
        acts_mean_non_outlier_tsla_amt,
        acts_measure_calc_dt,
        acts_mdn_abslt_dvn_thrshld_amt,
        acts_mdn_abslt_dvn_tsla_amt,
        acts_outlier_flg,
        acts_mdn_tsla_amt,
        tsla_mins_cnt,
        worktime_adjusted_for_acts_cnt,
        current_keyword_id,
        fiscal_month_closed_status_cd,
        service_request_status_name,
        service_request_status_id_int,
        change_csco_wrkr_prty_key,
        change_delivery_channel_name,
        change_tac_name,
        change_wg_master_theater_name,
        sk_change_dw_workgroup_key,
        change_workgroup_name,
        sk_change_wg_natural_key,
        change_workgroup_theater_name,
        dw_case_activity_key_id,
        bk_service_request_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_activity_tac_acts
)

SELECT * FROM final
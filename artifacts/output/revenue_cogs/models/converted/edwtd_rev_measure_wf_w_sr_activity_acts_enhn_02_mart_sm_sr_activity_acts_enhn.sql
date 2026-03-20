{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sr_activity_acts_enhn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_SR_ACTIVITY_ACTS_ENHN',
        'target_table': 'SM_SR_ACTIVITY_ACTS_ENHN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.823204+00:00'
    }
) }}

WITH 

source_sm_sr_activity_acts_enhn AS (
    SELECT
        sr_activity_acts_key,
        bk_fscl_cal_cd,
        bk_fscl_mth_num_int,
        bk_fscl_yr_num_int,
        bk_activity_cd_int,
        dw_case_activity_key_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_sr_activity_acts_enhn') }}
),

source_w_sr_activity_acts_enhn AS (
    SELECT
        sr_activity_acts_key,
        bk_fscl_cal_cd,
        bk_fscl_mth_num_int,
        bk_fscl_yr_num_int,
        bk_activity_cd_int,
        activity_adtnl_info_txt,
        activity_dtm,
        dv_activity_dt,
        activity_name,
        acts_abslt_dvtn_tsla_mins_cnt,
        acts_activity_name,
        acts_consistency_constant_amt,
        acts_mean_non_outlier_tsla_amt,
        acts_measure_calc_dt,
        acts_mdn_abslt_dvtn_thrsld_amt,
        acts_mdn_abslt_dvtn_tsla_amt,
        acts_outlier_flg,
        acts_median_tsla_amt,
        tsla_minutes_cnt,
        worktime_adjstd_for_acts_cnt,
        current_keyword_id,
        fscl_mth_closed_status_cd,
        sr_status_name,
        sr_status_id_int,
        change_csco_wrkr_prty_key,
        change_delivery_channel_name,
        change_tac_name,
        change_wg_master_theater_name,
        sk_change_dw_wg_key,
        change_workgroup_name,
        sk_change_wg_natural_key,
        change_wg_theater_name,
        dw_case_activity_key_id,
        bk_sr_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sr_activity_acts_enhn') }}
),

final AS (
    SELECT
        sr_activity_acts_key,
        bk_fscl_cal_cd,
        bk_fscl_mth_num_int,
        bk_fscl_yr_num_int,
        bk_activity_cd_int,
        dw_case_activity_key_id,
        edw_create_dtm,
        edw_create_user
    FROM source_w_sr_activity_acts_enhn
)

SELECT * FROM final
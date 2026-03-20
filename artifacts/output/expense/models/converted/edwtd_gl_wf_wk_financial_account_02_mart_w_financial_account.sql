{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_account', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_ACCOUNT',
        'target_table': 'W_FINANCIAL_ACCOUNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.097432+00:00'
    }
) }}

WITH 

source_n_cisco_worker_party_tv AS (
    SELECT
        cisco_worker_party_key,
        start_tv_date,
        end_tv_date,
        cisco_worker_party_type,
        bk_employee_id,
        cisco_email_address,
        cec_id,
        hire_date,
        job_family_code,
        job_family_description,
        public_job_title,
        shift_code,
        support_organization_code,
        work_eligibility_flag,
        work_type_code,
        supervisor_party_key,
        location_code,
        ru_bill_rate,
        ru_current_rate,
        ru_vendor_category_code,
        ru_dv_vendor_category_descr,
        ru_vendor_company_name,
        ru_vendor_description,
        ru_temp_category_code,
        ru_temp_source_code,
        ru_temp_type_code,
        ru_full_time_code,
        ru_director_code,
        ru_disabled_flag,
        ru_disabled_veteran_flag,
        ru_employee_class_code,
        ru_employee_status_code,
        ru_employee_type_code,
        ru_dv_grade_entry_date,
        ru_highest_educationl_lvl_code,
        ru_dv_job_entry_date,
        ru_manager_code,
        ru_military_status_code,
        ru_oncall_flag,
        ru_employee_type,
        ru_cs_email_address,
        ru_job_code,
        ru_temporary_jobtitle_name,
        ru_cs_temp_source,
        ru_original_hire_date,
        ru_rehire_date,
        ru_wpr_room_id,
        ru_hire_start_date,
        ru_referral_source_description,
        ru_last_day_worked_date,
        ru_termination_date,
        ru_contract_end_date,
        ru_service_date,
        ru_next_service_date,
        ru_next_anniversary_date,
        ru_regular_fte_amount,
        ru_work_phone_number,
        ru_birthdate_day_number,
        ru_birthdate_month_number,
        ru_wpr_floor_id,
        ru_mail_stop_code,
        ru_referral_source_code,
        ru_req_id,
        ru_months_of_service_count,
        ru_years_of_service_count,
        ru_employment_type_code,
        ru_reg_temp_code,
        ru_company_code,
        ru_department_code,
        ru_building_id,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'n_cisco_worker_party_tv') }}
),

source_st_si_account_info AS (
    SELECT
        batch_id,
        account_value,
        account_name,
        account_type_id,
        parent_account_value,
        local_balancesheet_owner_id,
        usage_description,
        start_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_account_info') }}
),

transformed_exp_ex_financial_account AS (
    SELECT
    batch_id,
    account_value,
    account_name,
    account_type_id,
    parent_account_value,
    local_balancesheet_owner_id,
    action_code,
    create_datetime,
    usage_description,
    start_date,
    last_update_date,
    'RI' AS exception_type
    FROM source_st_si_account_info
),

transformed_exp_w_financial_account AS (
    SELECT
    account_value,
    account_name,
    account_group_id,
    financial_account_type_code,
    parent_account_value,
    local_balancesheet_owner_id,
    edw_create_datetime,
    start_tv_date,
    end_tv_date,
    action_code,
    dml_type,
    parent_role,
    start_date,
    rank_index,
    financial_acct_display_seq_int,
    controllable_account_flg,
    exclude_it_allocations_flg,
    'NE' AS error_check
    FROM transformed_exp_ex_financial_account
),

final AS (
    SELECT
        bk_financial_account_code,
        ru_bk_parent_fin_acct_code,
        account_group_id,
        start_tv_date,
        end_tv_date,
        financial_account_type,
        financial_account_description,
        financial_acct_start_date,
        global_account_owner_party_key,
        edw_create_datetime,
        edw_update_datetime,
        parent_role,
        edw_update_user,
        edw_create_user,
        financial_acct_display_seq_int,
        controllable_account_flg,
        action_code,
        dml_type,
        exclude_it_allocations_flg
    FROM transformed_exp_w_financial_account
)

SELECT * FROM final
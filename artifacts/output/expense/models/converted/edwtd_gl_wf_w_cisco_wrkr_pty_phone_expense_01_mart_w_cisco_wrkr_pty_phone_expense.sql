{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_cisco_wrkr_pty_phone_expense', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_CISCO_WRKR_PTY_PHONE_EXPENSE',
        'target_table': 'W_CISCO_WRKR_PTY_PHONE_EXPENSE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.125482+00:00'
    }
) }}

WITH 

source_n_cisco_worker_party AS (
    SELECT
        cisco_worker_party_key,
        cisco_worker_party_type,
        bk_employee_id,
        cisco_email_address,
        cec_id,
        bk_worker_type_cd,
        supervisor_party_key,
        ru_employee_status_code,
        hr_job_type_key,
        edw_create_datetime,
        edw_update_datetime,
        bk_department_code,
        bk_company_code,
        bk_fiscal_year_number_int,
        bk_fiscal_week_number_int,
        bk_fiscal_calendar_cd,
        dd_primary_name,
        referral_source_descr,
        ru_bk_emplymnt_cat_cntngncy_cd,
        public_job_title,
        work_type_code,
        ru_regular_fte_amount,
        edw_create_user,
        edw_update_user
    FROM {{ source('raw', 'n_cisco_worker_party') }}
),

source_st_eman_mobility_spend_all AS (
    SELECT
        emplid,
        cec_userid,
        dept_name,
        deptid,
        vendor,
        service_type,
        telnum,
        bill_month,
        amount_usd,
        lastrefreshdate,
        created_datetime,
        action_code
    FROM {{ source('raw', 'st_eman_mobility_spend_all') }}
),

final AS (
    SELECT
        bk_mobile_telephone_num,
        bk_billing_cycle_start_dt,
        bk_mobile_carrier_name,
        bk_bill_to_dept_cd,
        bk_bill_to_company_cd,
        bk_mobile_plan_type_cd,
        cisco_worker_party_key,
        billed_usd_amt,
        source_refresh_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_eman_mobility_spend_all
)

SELECT * FROM final
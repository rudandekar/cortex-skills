{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_mt_erp_sales_rep', 'batch', 'edwtd_party'],
    meta={
        'source_workflow': 'wf_m_W_MT_ERP_SALES_REP',
        'target_table': 'W_MT_ERP_SALES_REP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.481498+00:00'
    }
) }}

WITH 

source_n_erp_sales_rep AS (
    SELECT
        sales_rep_number,
        erp_sales_rep_type,
        erp_sales_rep_name,
        ru_sales_terr_team_name,
        ru_cisco_worker_party_key,
        ss_code,
        edw_create_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        sales_rep_active_end_dt,
        sales_rep_active_start_dt,
        salesrep_status_cd,
        ru_team_type_cd,
        ru_sales_rep_type_cd,
        ru_team_descr,
        ru_sk_emp_non_emp_id_int,
        source_deleted_flg
    FROM {{ source('raw', 'n_erp_sales_rep') }}
),

source_n_cisco_worker_party_tv AS (
    SELECT
        cisco_worker_party_key,
        cisco_worker_party_type,
        bk_employee_id,
        cisco_email_address,
        start_tv_date,
        end_tv_date,
        bk_worker_type_cd,
        supervisor_party_key,
        ru_employee_status_code,
        ru_regular_fte_amount,
        edw_create_datetime,
        edw_update_datetime,
        bk_department_code,
        bk_company_code,
        dd_primary_name,
        referral_source_descr,
        ru_bk_emplymnt_cat_cntngncy_cd,
        public_job_title,
        work_type_code
    FROM {{ source('raw', 'n_cisco_worker_party_tv') }}
),

final AS (
    SELECT
        sales_rep_num,
        erp_sales_rep_type,
        erp_sales_rep_name,
        sales_terr_team_name,
        cwp_sorted_primary_name,
        ep_cisco_worker_party_key,
        ss_cd,
        sales_rep_active_end_dt,
        sales_rep_active_start_dt,
        salesrep_status_cd,
        bk_employee_id,
        cisco_email_address,
        cec_id,
        public_job_title,
        referral_source_descr,
        employee_status_cd,
        bk_department_cd,
        dd_primary_name,
        bk_company_cd,
        ru_bk_emplymnt_cat_cntngncy_cd,
        work_type_code,
        cisco_worker_party_type,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm,
        edw_update_user,
        team_type_cd,
        sales_rep_type_cd,
        team_descr,
        source_deleted_flg,
        action_code,
        dml_type
    FROM source_n_cisco_worker_party_tv
)

SELECT * FROM final
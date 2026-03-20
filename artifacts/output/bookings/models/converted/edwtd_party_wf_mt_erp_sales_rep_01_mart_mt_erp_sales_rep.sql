{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_erp_sales_rep', 'batch', 'edwtd_party'],
    meta={
        'source_workflow': 'wf_m_MT_ERP_SALES_REP',
        'target_table': 'MT_ERP_SALES_REP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.340613+00:00'
    }
) }}

WITH 

source_w_mt_erp_sales_rep AS (
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
    FROM {{ source('raw', 'w_mt_erp_sales_rep') }}
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
        source_deleted_flg
    FROM source_w_mt_erp_sales_rep
)

SELECT * FROM final
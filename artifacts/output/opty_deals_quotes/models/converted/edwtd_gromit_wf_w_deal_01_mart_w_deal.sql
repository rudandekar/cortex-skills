{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal', 'batch', 'edwtd_gromit'],
    meta={
        'source_workflow': 'wf_m_W_DEAL',
        'target_table': 'W_DEAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.992525+00:00'
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
        ru_regular_fte_amount
    FROM {{ source('raw', 'n_cisco_worker_party') }}
),

source_n_opportunity AS (
    SELECT
        opportunity_key,
        dv_deal_id_int,
        bk_opportunity_num,
        opportunity_name,
        non_standard_opportunity_cd,
        non_std_terms_conditions_cd,
        opportunity_product_type_cd,
        sales_territory_key,
        opportunity_created_by_user_id,
        opportunity_created_dtm,
        sfdc_theater_name,
        opportunity_updated_by_user_id,
        sfdc_opportunity_id,
        opportunity_update_dtm,
        sk_opportunity_id_int,
        bk_iso_currency_cd,
        sales_path_cd,
        opportunity_account_key,
        bk_sales_terr_assignment_name,
        bk_opportunity_user_login_id,
        bk_opportunity_usr_domain_name,
        sales_account_group_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        installed_network_base_cd,
        solution_positioning_name,
        probability_of_close_pct,
        sales_rep_number,
        product_enhancement_cd,
        bk_sls_terr_assignment_type_cd,
        bk_opportunity_type_cd
    FROM {{ source('raw', 'n_opportunity') }}
),

source_st_dmr_gromit_deal_header AS (
    SELECT
        batch_id,
        deal_object_id,
        opty_number,
        deal_name,
        account_name,
        theater,
        sla_flag,
        standard_nonstandard,
        opportunity_owner,
        deal_service_value,
        deal_status,
        market_segment,
        deal_last_update_date,
        deal_registration_date,
        approved_date_last,
        expiration_date,
        create_datetime,
        action_cd
    FROM {{ source('raw', 'st_dmr_gromit_deal_header') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_deal_status_cd,
        deal_created_dtm,
        opportunity_key,
        expected_deal_booking_usd_amt,
        ru_fn_cntlr_csco_wrkr_prty_key,
        ru_lgl_asrr_csco_wrkr_prty_key,
        ru_src_rptd_dl_svc_cst_usd_amt,
        ru_approved_dt,
        deal_expiration_dt,
        deal_type,
        approved_role,
        deal_last_update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_dmr_gromit_deal_header
)

SELECT * FROM final
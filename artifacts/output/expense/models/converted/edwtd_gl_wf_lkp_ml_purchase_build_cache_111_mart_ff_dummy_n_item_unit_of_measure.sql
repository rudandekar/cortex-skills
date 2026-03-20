{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ml_lkp_source_system_codes', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ML_LKP_SOURCE_SYSTEM_CODES',
        'target_table': 'FF_DUMMY_N_ITEM_UNIT_OF_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.004255+00:00'
    }
) }}

WITH 

source_el_fnd_user AS (
    SELECT
        created_by,
        creation_date,
        description,
        email_address,
        employee_id,
        end_date,
        gcn_code_combination_id,
        global_name,
        ges_update_date,
        last_update_date,
        last_updated_by,
        start_date,
        supplier_id,
        user_id,
        user_name,
        cms_replication_date,
        cms_replication_number
    FROM {{ source('raw', 'el_fnd_user') }}
),

source_el_per_all_people_f AS (
    SELECT
        applicant_number,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute16,
        attribute17,
        attribute18,
        attribute19,
        attribute2,
        attribute20,
        attribute21,
        attribute22,
        attribute23,
        attribute24,
        attribute25,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        background_check_status,
        business_group_id,
        comment_id,
        coord_ben_no_cvg_flag,
        created_by,
        creation_date,
        current_applicant_flag,
        current_emp_or_apl_flag,
        current_employee_flag,
        date_of_birth,
        dpdnt_vlntry_svce_flag,
        effective_end_date,
        effective_start_date,
        email_address,
        employee_number,
        expense_check_send_to_address,
        first_name,
        full_name,
        last_name,
        last_update_date,
        last_updated_by,
        marital_status,
        middle_names,
        national_identifier,
        object_version_number,
        on_military_service,
        order_name,
        original_date_of_hire,
        party_id,
        per_information_category,
        person_id,
        person_type_id,
        registered_disabled_flag,
        rehire_reason,
        rehire_recommendation,
        resume_exists,
        second_passport_exists,
        sex,
        start_date,
        title_of_person,
        work_telephone,
        cms_replication_date,
        cms_replication_number,
        ges_update_date,
        global_name,
        source_deleted_flg,
        ges_delete_date
    FROM {{ source('raw', 'el_per_all_people_f') }}
),

source_n_item_unit_of_measure AS (
    SELECT
        bk_item_unit_of_measure_cd,
        item_unit_of_measure_type_cd,
        base_unit_conversion_rt,
        item_unit_of_measure_descr,
        item_unit_of_measure_class_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_item_unit_of_measure') }}
),

source_n_iso_currency_tv AS (
    SELECT
        bk_iso_currency_code,
        start_tv_date,
        end_tv_date,
        iso_currency_name,
        sk_currency_code,
        ss_code,
        iso_currency_enabled_flag,
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        edw_create_user
    FROM {{ source('raw', 'n_iso_currency_tv') }}
),

source_n_ap_vndr_party_loc_asgmt AS (
    SELECT
        locator_key,
        ap_vendor_party_key,
        assignment_type_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_ap_vndr_party_loc_asgmt') }}
),

source_n_fiscal_month AS (
    SELECT
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        fiscal_month_start_date,
        fiscal_month_end_date,
        fiscal_month_close_date,
        dv_fiscal_month_name,
        bk_fiscal_quarter_number_int,
        bk_fiscal_calendar_code,
        dv_current_fiscal_month_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        dv_fiscal_month_age,
        dv_previous_fscl_month_number,
        dv_fiscal_ytm_flag,
        dv_fiscal_qtm_flag,
        dv_prev_fiscal_year_number,
        dv_previous_fscl_month_flag,
        dv_prev_yr_curr_fscl_mnth_flag,
        dv_number_of_fiscal_week_count,
        eo_fiscal_month_key
    FROM {{ source('raw', 'n_fiscal_month') }}
),

source_el_hr_all_org_units AS (
    SELECT
        organization_id,
        global_name,
        name,
        date_from,
        date_to,
        ges_update_date,
        attribute4,
        attribute16,
        attribute18
    FROM {{ source('raw', 'el_hr_all_org_units') }}
),

source_n_erp_freight_terms_tv AS (
    SELECT
        bk_freight_terms_code,
        start_tv_date,
        end_tv_date,
        freight_terms_name,
        freight_terms_description,
        freight_terms_enabled_flag,
        freight_terms_start_active_dtm,
        freight_terms_end_active_dtm,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime
    FROM {{ source('raw', 'n_erp_freight_terms_tv') }}
),

source_el_po_line_types_tl AS (
    SELECT
        creation_date,
        description,
        language_code,
        last_update_date,
        line_type,
        line_type_id,
        source_lang,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'el_po_line_types_tl') }}
),

source_n_source_system_codes AS (
    SELECT
        source_system_code,
        source_system_name,
        database_name,
        company,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user,
        global_name,
        gmt_offset
    FROM {{ source('raw', 'n_source_system_codes') }}
),

source_n_accts_pybl_vndr_prty AS (
    SELECT
        ap_vendor_party_key,
        default_payment_currency_cd,
        ap_vendor_name,
        default_invoice_currency_cd,
        bk_ap_vendor_num,
        sk_vendor_id_int,
        bk_ss_cd,
        vendor_type_cd,
        paid_date_basis_cd,
        terms_date_basis_cd,
        match_option_cd,
        active_start_dtm,
        active_end_dtm,
        enabled_flg,
        bk_payment_term_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        parent_vendor_name,
        cisco_worker_party_key
    FROM {{ source('raw', 'n_accts_pybl_vndr_prty') }}
),

source_n_accts_pybl_vndr_site_prty AS (
    SELECT
        ap_vendor_site_party_key,
        ap_vendor_party_key,
        bk_operating_unit_name_cd,
        bk_carrier_id,
        bk_ap_vendor_site_cd,
        bk_ap_vendor_num,
        ap_gl_account_key,
        prepay_gl_account_key,
        future_dated_pymt_gl_acct_key,
        default_payment_currency_cd,
        fob_point_cd,
        default_invoice_currency_cd,
        freight_terms_cd,
        purchasing_site_flg,
        rfq_only_site_flg,
        pay_site_flg,
        payment_method_cd,
        paid_date_basis_cd,
        match_option_cd,
        inactive_dtm,
        bk_payment_term_cd,
        sk_vendor_site_id_int,
        ss_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_accts_pybl_vndr_site_prty') }}
),

final AS (
    SELECT
        key1
    FROM source_n_accts_pybl_vndr_site_prty
)

SELECT * FROM final
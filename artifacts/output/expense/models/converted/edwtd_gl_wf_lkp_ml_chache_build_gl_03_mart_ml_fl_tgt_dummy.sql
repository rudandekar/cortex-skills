{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_lkp_ml_el_per_all_people_f', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_lkp_ML_EL_PER_ALL_PEOPLE_F',
        'target_table': 'ML_FL_TGT_DUMMY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.788141+00:00'
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

transformed_exptrans AS (
    SELECT
    employee_number,
    object_version_number
    FROM source_el_per_all_people_f
),

final AS (
    SELECT
        dummy
    FROM transformed_exptrans
)

SELECT * FROM final
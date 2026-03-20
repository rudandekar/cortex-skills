{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ap_bank_branch', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_AP_BANK_BRANCH',
        'target_table': 'SM_AP_BANK_BRANCH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.599335+00:00'
    }
) }}

WITH 

source_st_mf_ap_bank_branches AS (
    SELECT
        batch_id,
        bank_branch_id,
        last_update_date,
        last_updated_by,
        bank_name,
        bank_branch_name,
        institution_type,
        description,
        address_line1,
        address_line2,
        address_line3,
        city,
        state,
        zip,
        province,
        country,
        area_code,
        phone,
        contact_first_name,
        contact_middle_name,
        contact_last_name,
        contact_prefix,
        contact_title,
        bank_num,
        last_update_login,
        creation_date,
        created_by,
        clearing_house_id,
        transmission_program_id,
        printing_program_id,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        address_style,
        bank_number,
        address_line4,
        county,
        eft_user_number,
        eft_swift_code,
        end_date,
        edi_id_number,
        bank_branch_type,
        bank_name_alt,
        bank_branch_name_alt,
        address_lines_alt,
        active_date,
        tp_header_id,
        ece_tp_location_code,
        payroll_bank_account_id,
        rfc_identifier,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_ap_bank_branches') }}
),

transformed_exp_w_ap_bank_branch AS (
    SELECT
    ap_bank_branch_key,
    bank_branch_name,
    bank_name,
    bank_branch_id,
    bank_num,
    global_name,
    source_deleted_flg,
    action_code,
    dml_type
    FROM source_st_mf_ap_bank_branches
),

final AS (
    SELECT
        ap_bank_branch_key,
        sk_bank_branch_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm
    FROM transformed_exp_w_ap_bank_branch
)

SELECT * FROM final
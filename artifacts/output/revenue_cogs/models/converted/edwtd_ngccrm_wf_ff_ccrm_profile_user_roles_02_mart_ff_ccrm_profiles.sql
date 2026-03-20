{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_profile_user_roles', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_PROFILE_USER_ROLES',
        'target_table': 'FF_CCRM_PROFILES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.339368+00:00'
    }
) }}

WITH 

source_ccrm_profile_user_roles AS (
    SELECT
        user_id,
        role_id,
        profile_id,
        last_updated_by,
        last_update_date
    FROM {{ source('raw', 'ccrm_profile_user_roles') }}
),

transformed_ccrm_profile_user_roles AS (
    SELECT
    user_id,
    role_id,
    profile_id,
    last_updated_by,
    last_update_date,
    'Batch_Id' AS batch_id,
    CURRENT_TIMESTAMP() AS current_datetime,
    'I' AS action_code
    FROM source_ccrm_profile_user_roles
),

final AS (
    SELECT
        batch_id,
        profile_id,
        global_deal_id,
        completeness_flag,
        profile_status,
        primary_reserve_type,
        prime_flag,
        agreement_name,
        cust_id,
        partner_name,
        fiscal_period,
        start_period,
        legal_contact,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_erp_update_date,
        version,
        first_install_date,
        rm_email_id,
        profile_source,
        profile_type,
        theater_id,
        area_id,
        revrec_status,
        corporate_flag,
        deferral_net_value,
        forecast_invalid_flag,
        customer_id,
        customer_name,
        legacy_flag,
        forecast_submit_flag,
        fcst_last_updated_by,
        fcst_last_update_date,
        merged_flag,
        profile_catg,
        discriminator,
        systematic_hold,
        defer_percent,
        assigned_on,
        category_cd,
        cogs_impact,
        operating_unit,
        sales_segment,
        accounting_rule,
        join_deal,
        deal_exp_date,
        global,
        originating_local,
        origin_dl_pro_created,
        originating_profile_id,
        originating_deal_id,
        reference_id,
        reference_name,
        reference_value,
        reopen_reason_code,
        ack_profile_status,
        accrule_updated_by,
        transaction_level,
        revenue_rule,
        sales_insen_prog,
        exp_rev_reg_date,
        ns_term_summary,
        profile_status_date,
        current_datetime,
        action_code
    FROM transformed_ccrm_profile_user_roles
)

SELECT * FROM final
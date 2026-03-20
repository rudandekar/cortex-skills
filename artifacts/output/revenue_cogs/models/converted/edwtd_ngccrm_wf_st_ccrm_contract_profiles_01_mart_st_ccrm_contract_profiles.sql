{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_contract_profiles', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_CONTRACT_PROFILES',
        'target_table': 'ST_CCRM_CONTRACT_PROFILES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.309107+00:00'
    }
) }}

WITH 

source_ccrm_profiles AS (
    SELECT
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
        ns_term_summary,
        profile_status_date
    FROM {{ source('raw', 'ccrm_profiles') }}
),

source_ccrm_cms_contracts AS (
    SELECT
        contract_id,
        contract_number,
        contract_version,
        contract_status,
        effective_date,
        expiration_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date
    FROM {{ source('raw', 'ccrm_cms_contracts') }}
),

source_ff_ccrm_contract_profiles AS (
    SELECT
        batch_id,
        contract_id,
        profile_id,
        created_by,
        creation_date,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'ff_ccrm_contract_profiles') }}
),

final AS (
    SELECT
        batch_id,
        contract_id,
        profile_id,
        created_by,
        creation_date,
        create_timestamp,
        action_code
    FROM source_ff_ccrm_contract_profiles
)

SELECT * FROM final
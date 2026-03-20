{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_profiles', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_PROFILES',
        'target_table': 'N_CCRM_PROFILE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.309656+00:00'
    }
) }}

WITH 

source_w_ccrm_profile AS (
    SELECT
        bk_ccrm_profile_id_int,
        agreement_name,
        profile_status_cd,
        cogs_impact_flg,
        source_reported_customer_name,
        profile_product_category_cd,
        profile_status_change_dt,
        profile_source_cd,
        revenue_recognition_status_cd,
        non_standard_terms_summary_txt,
        completeness_flg,
        with_reference_role,
        with_originating_deal_role,
        with_deal_role,
        sales_territory_key,
        operating_unit_name_cd,
        ru_originating_opportunity_key,
        ru_opportunity_key,
        ru_reference_type_cd,
        ru_src_rprtd_ref_value_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_deal_expiration_dt,
        ru_joint_deal_flg,
        accounting_scope_summary_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_profile') }}
),

final AS (
    SELECT
        bk_ccrm_profile_id_int,
        agreement_name,
        profile_status_cd,
        cogs_impact_flg,
        source_reported_customer_name,
        profile_product_category_cd,
        profile_status_change_dt,
        profile_source_cd,
        revenue_recognition_status_cd,
        non_standard_terms_summary_txt,
        completeness_flg,
        with_reference_role,
        with_originating_deal_role,
        with_deal_role,
        sales_territory_key,
        operating_unit_name_cd,
        ru_originating_opportunity_key,
        ru_opportunity_key,
        ru_reference_type_cd,
        ru_src_rprtd_ref_value_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_deal_expiration_dt,
        ru_joint_deal_flg,
        accounting_scope_summary_cd
    FROM source_w_ccrm_profile
)

SELECT * FROM final
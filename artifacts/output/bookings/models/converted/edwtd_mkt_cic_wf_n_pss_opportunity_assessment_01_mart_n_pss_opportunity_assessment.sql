{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pss_opportunity_assessment', 'batch', 'edwtd_mkt_cic'],
    meta={
        'source_workflow': 'wf_m_N_PSS_OPPORTUNITY_ASSESSMENT',
        'target_table': 'N_PSS_OPPORTUNITY_ASSESSMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.393806+00:00'
    }
) }}

WITH 

source_w_pss_opportunity_assessment AS (
    SELECT
        pss_opportunity_assessment_key,
        bk_pss_agent_worker_party_key,
        bk_opportunity_key,
        sales_territory_key,
        bk_pss_assessment_created_dtm,
        iso_currency_cd,
        pss_team_name,
        pss_expected_book_dt,
        forecast_status_cd,
        pss_assessment_status_cd,
        sync_with_opportunity_flg,
        sync_status_descr,
        business_driver_descr,
        solution_offer_descr,
        application_environment_descr,
        source_deleted_flg,
        sfdc_workspace_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        partner_country_party_key,
        pss_optional_cmt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_pss_opportunity_assessment') }}
),

final AS (
    SELECT
        pss_opportunity_assessment_key,
        bk_pss_agent_worker_party_key,
        bk_opportunity_key,
        sales_territory_key,
        bk_pss_assessment_created_dtm,
        iso_currency_cd,
        pss_team_name,
        pss_expected_book_dt,
        forecast_status_cd,
        pss_assessment_status_cd,
        sync_with_opportunity_flg,
        sync_status_descr,
        business_driver_descr,
        solution_offer_descr,
        application_environment_descr,
        source_deleted_flg,
        sfdc_workspace_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        partner_country_party_key,
        pss_optional_cmt
    FROM source_w_pss_opportunity_assessment
)

SELECT * FROM final
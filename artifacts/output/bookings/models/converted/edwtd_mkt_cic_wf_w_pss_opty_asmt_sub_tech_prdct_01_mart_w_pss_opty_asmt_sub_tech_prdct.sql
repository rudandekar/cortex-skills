{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_pss_opty_asmt_sub_tech_prdct', 'batch', 'edwtd_mkt_cic'],
    meta={
        'source_workflow': 'wf_m_W_PSS_OPTY_ASMT_SUB_TECH_PRDCT',
        'target_table': 'W_PSS_OPTY_ASMT_SUB_TECH_PRDCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.142419+00:00'
    }
) }}

WITH 

source_st_at_workspace__c AS (
    SELECT
        etl_id,
        delete_flag,
        id,
        isdeleted,
        name,
        currencyisocode,
        createddate,
        createdbyid,
        lastmodifieddate,
        lastmodifiedbyid,
        systemmodstamp,
        mayedit,
        islocked,
        at_expected_book_date__c,
        at_product__c,
        pss__c,
        at_expected_product__c,
        at_expected_service__c,
        at_forecast_status__c,
        at_partner__c,
        at_sub_technology__c,
        at_technology__c,
        at_team__c,
        at_optional_1__c,
        at_optional_2__c,
        at_next_step__c,
        at_other_partner__c,
        opportunity__c,
        at_next_step_date__c,
        at_competitor__c,
        at_other_competitor__c,
        at_flags__c,
        account_name__c,
        at_no_of_days_overdue__c,
        deal_id__c,
        expected_book_date__c,
        expected_product_000__c,
        expected_service_000__c,
        forecast_status__c,
        opportunity_status__c,
        stage__c,
        pss_workspace_status__c,
        forecast_position_id__c,
        fy_start_date_pss__c,
        fy_copy_pss__c,
        fiscal_week_pss__c,
        fy_extra_week_pss__c,
        fiscal_year_pss__c,
        fiscal_quarter_pss__c,
        fiscal_month_of_qtr_pss__c,
        fiscal_month_pss__c,
        fiscal_week_of_month_pss__c,
        fiscal_period_pss__c,
        business_driver__c,
        solution_offer__c,
        application__c,
        additional_use_case_info__c,
        deployment_no__c,
        data_source_name__c,
        pss_email__c,
        pss_alias__c,
        sync_status__c,
        sync_with_opportunity__c,
        opportunity_territory_levels,
        pss_expected_total_value_000_s,
        batch_id,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_at_workspace__c') }}
),

final AS (
    SELECT
        bk_pss_technology_name,
        bk_pss_sub_technology_name,
        bk_pss_product_id,
        pss_opportunity_assessment_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_at_workspace__c
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ar_trx_2tier_rule', 'batch', 'edwtd_2tier'],
    meta={
        'source_workflow': 'wf_m_W_AR_TRX_2TIER_RULE',
        'target_table': 'W_AR_TRX_2TIER_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.005546+00:00'
    }
) }}

WITH 

source_st_cg_xxcfir_revenue_rules_all AS (
    SELECT
        batch_id,
        application_name,
        application_id,
        rule_id,
        org_id,
        start_period_year,
        end_period_year,
        start_period_num,
        end_period_num,
        dist_type,
        theatre,
        sequence_number,
        percentage,
        account_1,
        dest_set_of_books_id,
        dest_currency_code,
        company_code,
        department,
        location,
        subaccount,
        project,
        last_update_date,
        last_updated_by,
        last_update_login,
        creation_date,
        created_by,
        element_type,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg_xxcfir_revenue_rules_all') }}
),

source_st_om_cfi_2tier_rules_all AS (
    SELECT
        account_no,
        company_code,
        created_by,
        creation_date,
        department,
        dest_currency_code,
        dest_set_of_books_id,
        dist_type,
        end_period_num,
        end_period_year,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        location,
        org_id,
        percentage,
        project,
        rule_id,
        rule_name,
        sequence_number,
        start_period_num,
        start_period_year,
        subaccount,
        theatre,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cfi_2tier_rules_all') }}
),

final AS (
    SELECT
        bk_financial_account_cd,
        bk_fiscal_calendar_cd,
        bk_start_fiscal_year_num_int,
        bk_start_fiscal_month_num_int,
        bk_cfi_theater_name,
        distribution_type_cd,
        two_tier_rule_name,
        reserve_percent_rt,
        set_of_books_key,
        operating_unit_name_cd,
        end_fiscal_year_num_int,
        end_fiscal_month_num_int,
        sk_rule_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_om_cfi_2tier_rules_all
)

SELECT * FROM final
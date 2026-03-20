{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_pa_bill_rates_all', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_BILL_RATES_ALL',
        'target_table': 'CSF_PA_BILL_RATES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.807232+00:00'
    }
) }}

WITH 

source_csf_pa_bill_rates_all AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        bill_rate_organization_id,
        std_bill_rate_schedule,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        start_date_active,
        person_id,
        job_id,
        expenditure_type,
        non_labor_resource,
        rate,
        bill_rate_unit,
        markup_percentage,
        end_date_active,
        org_id,
        bill_rate_sch_id,
        job_group_id,
        rate_currency_code,
        resource_class_code,
        res_class_organization_id,
        integration_id,
        expenditure_category,
        revenue_category,
        fee_event_type
    FROM {{ source('raw', 'csf_pa_bill_rates_all') }}
),

source_stg_csf_pa_bill_rates_all AS (
    SELECT
        bill_rate_organization_id,
        std_bill_rate_schedule,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        start_date_active,
        person_id,
        job_id,
        expenditure_type,
        non_labor_resource,
        rate,
        bill_rate_unit,
        markup_percentage,
        end_date_active,
        org_id,
        bill_rate_sch_id,
        job_group_id,
        rate_currency_code,
        resource_class_code,
        res_class_organization_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_bill_rates_all') }}
),

transformed_exp_csf_pa_bill_rates_all AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    bill_rate_organization_id,
    std_bill_rate_schedule,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    start_date_active,
    person_id,
    job_id,
    expenditure_type,
    non_labor_resource,
    rate,
    bill_rate_unit,
    markup_percentage,
    end_date_active,
    org_id,
    bill_rate_sch_id,
    job_group_id,
    rate_currency_code,
    resource_class_code,
    res_class_organization_id,
    integration_id,
    expenditure_category,
    revenue_category,
    fee_event_type
    FROM source_stg_csf_pa_bill_rates_all
),

final AS (
    SELECT
        bill_rate_organization_id,
        std_bill_rate_schedule,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        start_date_active,
        person_id,
        job_id,
        expenditure_type,
        non_labor_resource,
        rate,
        bill_rate_unit,
        markup_percentage,
        end_date_active,
        org_id,
        bill_rate_sch_id,
        job_group_id,
        rate_currency_code,
        resource_class_code,
        res_class_organization_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_bill_rates_all
)

SELECT * FROM final
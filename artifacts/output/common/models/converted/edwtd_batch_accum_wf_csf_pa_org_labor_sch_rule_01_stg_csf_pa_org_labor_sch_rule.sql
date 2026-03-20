{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_pa_org_labor_sch_rule', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_ORG_LABOR_SCH_RULE',
        'target_table': 'STG_CSF_PA_ORG_LABOR_SCH_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.555730+00:00'
    }
) }}

WITH 

source_stg_csf_pa_org_labor_sch_rule AS (
    SELECT
        org_labor_sch_rule_id,
        organization_id,
        org_id,
        labor_costing_rule,
        cost_rate_sch_id,
        forecast_cost_rate_sch_id,
        overtime_project_id,
        overtime_task_id,
        acct_rate_date_code,
        acct_rate_type,
        acct_exchange_rate,
        start_date_active,
        end_date_active,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_org_labor_sch_rule') }}
),

source_csf_pa_org_labor_sch_rule AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        org_labor_sch_rule_id,
        organization_id,
        org_id,
        labor_costing_rule,
        cost_rate_sch_id,
        forecast_cost_rate_sch_id,
        overtime_project_id,
        overtime_task_id,
        acct_rate_date_code,
        acct_rate_type,
        acct_exchange_rate,
        start_date_active,
        end_date_active,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        base_hours,
        rbc_element_type_id
    FROM {{ source('raw', 'csf_pa_org_labor_sch_rule') }}
),

transformed_exp_csf_pa_org_labor_sch_rule AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    org_labor_sch_rule_id,
    organization_id,
    org_id,
    labor_costing_rule,
    cost_rate_sch_id,
    forecast_cost_rate_sch_id,
    overtime_project_id,
    overtime_task_id,
    acct_rate_date_code,
    acct_rate_type,
    acct_exchange_rate,
    start_date_active,
    end_date_active,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    base_hours,
    rbc_element_type_id
    FROM source_csf_pa_org_labor_sch_rule
),

final AS (
    SELECT
        org_labor_sch_rule_id,
        organization_id,
        org_id,
        labor_costing_rule,
        cost_rate_sch_id,
        forecast_cost_rate_sch_id,
        overtime_project_id,
        overtime_task_id,
        acct_rate_date_code,
        acct_rate_type,
        acct_exchange_rate,
        start_date_active,
        end_date_active,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_org_labor_sch_rule
)

SELECT * FROM final
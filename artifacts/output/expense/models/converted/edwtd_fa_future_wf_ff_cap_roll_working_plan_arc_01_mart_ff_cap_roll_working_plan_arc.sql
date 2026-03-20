{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cap_roll_working_plan_arc', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_FF_CAP_ROLL_WORKING_PLAN_ARC',
        'target_table': 'FF_CAP_ROLL_WORKING_PLAN_ARC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.791517+00:00'
    }
) }}

WITH 

source_cap_roll_working_plan_arc AS (
    SELECT
        project_id,
        amount,
        period_id,
        account_id,
        org_id,
        country_department,
        cip_flag,
        period_name,
        quarter_id,
        planning_id
    FROM {{ source('raw', 'cap_roll_working_plan_arc') }}
),

transformed_exp_cap_roll_working_plan_arc AS (
    SELECT
    project_id,
    amount,
    period_id,
    account_id,
    org_id,
    country_department,
    cip_flag,
    period_name,
    quarter_id,
    planning_id,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cap_roll_working_plan_arc
),

final AS (
    SELECT
        batch_id,
        project_id,
        amount,
        period_id,
        account_id,
        org_id,
        country_department,
        cip_flag,
        period_name,
        quarter_id,
        planning_id,
        create_datetime,
        action_code
    FROM transformed_exp_cap_roll_working_plan_arc
)

SELECT * FROM final
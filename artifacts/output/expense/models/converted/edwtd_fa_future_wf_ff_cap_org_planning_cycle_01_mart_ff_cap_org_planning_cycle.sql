{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cap_org_planning_cycle', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_FF_CAP_ORG_PLANNING_CYCLE',
        'target_table': 'FF_CAP_ORG_PLANNING_CYCLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.771840+00:00'
    }
) }}

WITH 

source_cap_org_planning_cycle AS (
    SELECT
        period_id,
        period_name,
        period_year,
        planning_cycle_id,
        budget_flag,
        quarter_id,
        planning_date,
        commit_flag,
        org_id,
        planning_id,
        quarter_num,
        fp_planning_cycle_id,
        planning_commit_flag,
        planning_period_id
    FROM {{ source('raw', 'cap_org_planning_cycle') }}
),

transformed_exp_cap_org_planning_cycle AS (
    SELECT
    period_id,
    period_name,
    period_year,
    planning_cycle_id,
    budget_flag,
    quarter_id,
    planning_date,
    commit_flag,
    org_id,
    planning_id,
    quarter_num,
    fp_planning_cycle_id,
    planning_commit_flag,
    planning_period_id,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cap_org_planning_cycle
),

final AS (
    SELECT
        batch_id,
        period_id,
        period_name,
        period_year,
        planning_cycle_id,
        budget_flag,
        quarter_id,
        planning_date,
        commit_flag,
        org_id,
        planning_id,
        quarter_num,
        fp_planning_cycle_id,
        planning_commit_flag,
        planning_period_id,
        create_datetime,
        action_code
    FROM transformed_exp_cap_org_planning_cycle
)

SELECT * FROM final
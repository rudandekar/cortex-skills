{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cap_org_planning_cycle', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_ST_CAP_ORG_PLANNING_CYCLE',
        'target_table': 'ST_CAP_ORG_PLANNING_CYCLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.966806+00:00'
    }
) }}

WITH 

source_ff_cap_org_planning_cycle AS (
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
    FROM {{ source('raw', 'ff_cap_org_planning_cycle') }}
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
    FROM source_ff_cap_org_planning_cycle
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cap_roll_working_plan_arc', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_ST_CAP_ROLL_WORKING_PLAN_ARC',
        'target_table': 'ST_CAP_ROLL_WORK_PLAN_ARC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.772166+00:00'
    }
) }}

WITH 

source_ff_cap_roll_working_plan_arc AS (
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
    FROM {{ source('raw', 'ff_cap_roll_working_plan_arc') }}
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
    FROM source_ff_cap_roll_working_plan_arc
)

SELECT * FROM final
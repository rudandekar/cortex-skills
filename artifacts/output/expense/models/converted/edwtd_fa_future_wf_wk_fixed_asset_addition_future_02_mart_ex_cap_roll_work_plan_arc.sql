{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_fixed_asset_addition_future', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_WK_FIXED_ASSET_ADDITION_FUTURE',
        'target_table': 'EX_CAP_ROLL_WORK_PLAN_ARC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.855255+00:00'
    }
) }}

WITH 

source_st_cap_roll_work_plan_arc AS (
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
    FROM {{ source('raw', 'st_cap_roll_work_plan_arc') }}
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
        action_code,
        exception_type
    FROM source_st_cap_roll_work_plan_arc
)

SELECT * FROM final
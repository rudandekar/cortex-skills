{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cap_roll_working_plan', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_ST_CAP_ROLL_WORKING_PLAN',
        'target_table': 'ST_CAP_ROLL_WORKING_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.982316+00:00'
    }
) }}

WITH 

source_ff_cap_roll_working_plan AS (
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
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_cap_roll_working_plan') }}
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
        create_datetime,
        action_code
    FROM source_ff_cap_roll_working_plan
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cap_roll_working_plan', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_FF_CAP_ROLL_WORKING_PLAN',
        'target_table': 'FF_CAP_ROLL_WORKING_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.712486+00:00'
    }
) }}

WITH 

source_cap_roll_working_plan AS (
    SELECT
        project_id,
        amount,
        period_id,
        account_id,
        org_id,
        country_department,
        cip_flag,
        period_name
    FROM {{ source('raw', 'cap_roll_working_plan') }}
),

transformed_exp_cap_roll_working_plan AS (
    SELECT
    project_id,
    amount,
    period_id,
    account_id,
    org_id,
    country_department,
    cip_flag,
    period_name,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cap_roll_working_plan
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
    FROM transformed_exp_cap_roll_working_plan
)

SELECT * FROM final
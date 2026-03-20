{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_hr_employee', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_HR_EMPLOYEE',
        'target_table': 'FF_CCRM_HR_EMPLOYEE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.454567+00:00'
    }
) }}

WITH 

source_ccrm_hr_employee AS (
    SELECT
        user_id,
        init_cap_fname,
        init_cap_lname,
        dept_name
    FROM {{ source('raw', 'ccrm_hr_employee') }}
),

transformed_exp_ccrm_hr_employee AS (
    SELECT
    user_id,
    init_cap_fname,
    init_cap_lname,
    dept_name,
    'BATCH_ID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_timestamp,
    'I' AS action_code
    FROM source_ccrm_hr_employee
),

final AS (
    SELECT
        batch_id,
        user_id,
        init_cap_fname,
        init_cap_lname,
        dept_name,
        create_timestamp,
        action_code
    FROM transformed_exp_ccrm_hr_employee
)

SELECT * FROM final
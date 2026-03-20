{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_hr_employee', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_HR_EMPLOYEE',
        'target_table': 'ST_CCRM_HR_EMPLOYEE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.230421+00:00'
    }
) }}

WITH 

source_ff_ccrm_hr_employee AS (
    SELECT
        batch_id,
        user_id,
        init_cap_fname,
        init_cap_lname,
        dept_name,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'ff_ccrm_hr_employee') }}
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
    FROM source_ff_ccrm_hr_employee
)

SELECT * FROM final
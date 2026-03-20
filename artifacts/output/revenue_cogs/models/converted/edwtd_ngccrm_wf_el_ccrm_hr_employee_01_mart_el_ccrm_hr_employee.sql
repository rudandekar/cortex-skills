{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccrm_hr_employee', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_EL_CCRM_HR_EMPLOYEE',
        'target_table': 'EL_CCRM_HR_EMPLOYEE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.340383+00:00'
    }
) }}

WITH 

source_st_ccrm_hr_employee AS (
    SELECT
        batch_id,
        user_id,
        init_cap_fname,
        init_cap_lname,
        dept_name,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ccrm_hr_employee') }}
),

final AS (
    SELECT
        user_id,
        init_cap_fname,
        init_cap_lname,
        dept_name,
        created_by,
        creation_datetime,
        last_updated_by,
        last_update_date
    FROM source_st_ccrm_hr_employee
)

SELECT * FROM final
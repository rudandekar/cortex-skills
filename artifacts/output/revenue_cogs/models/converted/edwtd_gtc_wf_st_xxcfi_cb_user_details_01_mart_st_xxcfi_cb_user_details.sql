{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_user_details', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_USER_DETAILS',
        'target_table': 'ST_XXCFI_CB_USER_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.741139+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_user_details AS (
    SELECT
        batch_id,
        user_id,
        cec_id,
        first_name,
        last_name,
        email_id,
        active_flag,
        start_date,
        end_date,
        department_name,
        created_by,
        created_date,
        modified_by,
        modified_date,
        phone,
        create_datetime,
        action_cd
    FROM {{ source('raw', 'ff_xxcfi_cb_user_details') }}
),

final AS (
    SELECT
        batch_id,
        user_id,
        cec_id,
        first_name,
        last_name,
        email_id,
        active_flag,
        start_date,
        end_date,
        department_name,
        created_by,
        created_date,
        modified_by,
        modified_date,
        phone,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_user_details
)

SELECT * FROM final
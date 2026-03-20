{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfi_cb_user_details', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFI_CB_USER_DETAILS',
        'target_table': 'EL_XXCFI_CB_USER_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.214611+00:00'
    }
) }}

WITH 

source_st_xxcfi_cb_user_details AS (
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
    FROM {{ source('raw', 'st_xxcfi_cb_user_details') }}
),

final AS (
    SELECT
        user_id,
        cec_id,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_st_xxcfi_cb_user_details
)

SELECT * FROM final
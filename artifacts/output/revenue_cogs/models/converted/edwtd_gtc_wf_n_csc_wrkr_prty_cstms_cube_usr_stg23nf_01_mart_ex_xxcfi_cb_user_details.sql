{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_csc_wrkr_prty_cstms_cube_usr_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_CSC_WRKR_PRTY_CSTMS_CUBE_USR_STG23NF',
        'target_table': 'EX_XXCFI_CB_USER_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.011275+00:00'
    }
) }}

WITH 

source_ex_xxcfi_cb_user_details AS (
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
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_user_details') }}
),

source_n_csc_wrkr_prty_cstms_cube_usr AS (
    SELECT
        cisco_worker_party_key,
        cube_user_effective_start_dt,
        cube_user_effective_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_csc_wrkr_prty_cstms_cube_usr') }}
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
        action_code,
        exception_type
    FROM source_n_csc_wrkr_prty_cstms_cube_usr
)

SELECT * FROM final
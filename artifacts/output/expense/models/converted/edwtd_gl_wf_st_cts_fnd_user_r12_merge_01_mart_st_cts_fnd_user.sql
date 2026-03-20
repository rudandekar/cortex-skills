{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cts_fnd_user_r12_merge', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CTS_FND_USER_R12_MERGE',
        'target_table': 'ST_CTS_FND_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.671542+00:00'
    }
) }}

WITH 

source_st_csf_fnd_user_r12 AS (
    SELECT
        batch_id,
        user_id,
        user_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        encrypted_foundation_password,
        encrypted_user_password,
        session_number,
        start_date,
        last_update_login,
        end_date,
        description,
        last_logon_date,
        password_date,
        password_accesses_left,
        password_lifespan_accesses,
        password_lifespan_days,
        employee_id,
        email_address,
        fax,
        customer_id,
        supplier_id,
        web_password,
        gcn_code_combination_id,
        person_party_id,
        user_guid,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_csf_fnd_user_r12') }}
),

final AS (
    SELECT
        batch_id,
        user_id,
        user_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        start_date,
        end_date,
        description,
        employee_id,
        email_address,
        supplier_id,
        gcn_code_combination_id,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM source_st_csf_fnd_user_r12
)

SELECT * FROM final
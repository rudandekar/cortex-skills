{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_fnd_user', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_FND_USER',
        'target_table': 'FF_MF_FND_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.927308+00:00'
    }
) }}

WITH 

source_mf_fnd_user AS (
    SELECT
        created_by,
        creation_date,
        customer_id,
        description,
        email_address,
        employee_id,
        encrypted_foundation_password,
        encrypted_user_password,
        end_date,
        fax,
        ges_update_date,
        global_name,
        last_logon_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        password_accesses_left,
        password_date,
        password_lifespan_accesses,
        password_lifespan_days,
        session_number,
        start_date,
        supplier_id,
        user_id,
        user_name,
        web_password,
        gcn_code_combination_id,
        cms_replication_date,
        cms_replication_number
    FROM {{ source('raw', 'mf_fnd_user') }}
),

transformed_exp_mf_fnd_user AS (
    SELECT
    created_by,
    creation_date,
    description,
    email_address,
    employee_id,
    end_date,
    gcn_code_combination_id,
    global_name,
    ges_update_date,
    last_update_date,
    last_updated_by,
    start_date,
    supplier_id,
    user_id,
    user_name,
    cms_replication_date,
    cms_replication_number,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_mf_fnd_user
),

final AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        description,
        email_address,
        employee_id,
        end_date,
        gcn_code_combination_id,
        global_name,
        ges_update_date,
        last_update_date,
        last_updated_by,
        start_date,
        supplier_id,
        user_id,
        user_name,
        cms_replication_date,
        cms_replication_number,
        create_datetime,
        action_code
    FROM transformed_exp_mf_fnd_user
)

SELECT * FROM final
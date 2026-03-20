{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cg1_fnd_user', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_CG1_FND_USER',
        'target_table': 'FF_CG1_FND_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.923889+00:00'
    }
) }}

WITH 

source_cg1_fnd_user AS (
    SELECT
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
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_fnd_user') }}
),

transformed_exp_cg1_fnd_user AS (
    SELECT
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
    ges_update_date,
    global_name,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_created_datetime,
    'I' AS o_action_code
    FROM source_cg1_fnd_user
),

final AS (
    SELECT
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
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM transformed_exp_cg1_fnd_user
)

SELECT * FROM final
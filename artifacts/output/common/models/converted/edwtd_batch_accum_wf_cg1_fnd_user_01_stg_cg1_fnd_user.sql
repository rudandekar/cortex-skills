{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_fnd_user', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_FND_USER',
        'target_table': 'STG_CG1_FND_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.970526+00:00'
    }
) }}

WITH 

source_cg1_fnd_user AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        user_id,
        user_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        encrypted_foundation_password,
        encrypted_user_password,
        session_number,
        start_date,
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
        user_guid,
        gcn_code_combination_id,
        person_party_id
    FROM {{ source('raw', 'cg1_fnd_user') }}
),

source_stg_cg1_fnd_user AS (
    SELECT
        created_by,
        creation_date,
        description,
        email_address,
        employee_id,
        end_date,
        gcn_code_combination_id,
        last_update_date,
        last_updated_by,
        start_date,
        supplier_id,
        user_id,
        user_name,
        source_commit_time,
        trail_file_name,
        source_dml_type,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_fnd_user') }}
),

transformed_exp_cg1_fnd_user AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
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
    gcn_code_combination_id
    FROM source_stg_cg1_fnd_user
),

final AS (
    SELECT
        created_by,
        creation_date,
        description,
        email_address,
        employee_id,
        end_date,
        gcn_code_combination_id,
        last_update_date,
        last_updated_by,
        start_date,
        supplier_id,
        user_id,
        user_name,
        source_commit_time,
        trail_file_name,
        source_dml_type,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_fnd_user
)

SELECT * FROM final
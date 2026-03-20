{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_fnd_user', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_FND_USER',
        'target_table': 'ST_MF_FND_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.724589+00:00'
    }
) }}

WITH 

source_ff_mf_fnd_user AS (
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
    FROM {{ source('raw', 'ff_mf_fnd_user') }}
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
    FROM source_ff_mf_fnd_user
)

SELECT * FROM final
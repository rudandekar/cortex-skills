{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_pa_project_customers', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_PROJECT_CUSTOMERS',
        'target_table': 'CSF_PA_PROJECT_CUSTOMERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.820618+00:00'
    }
) }}

WITH 

source_csf_pa_project_customers AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        project_id,
        customer_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        project_relationship_code,
        customer_bill_split,
        bill_to_address_id,
        ship_to_address_id,
        inv_currency_code,
        inv_rate_type,
        inv_rate_date,
        inv_exchange_rate,
        allow_inv_user_rate_type_flag,
        bill_another_project_flag,
        receiver_task_id,
        record_version_number,
        project_party_id,
        retention_level_code,
        bill_to_customer_id,
        ship_to_customer_id,
        default_top_task_cust_flag
    FROM {{ source('raw', 'csf_pa_project_customers') }}
),

source_stg_csf_pa_project_customers AS (
    SELECT
        project_id,
        customer_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        project_relationship_code,
        customer_bill_split,
        bill_to_address_id,
        ship_to_address_id,
        inv_currency_code,
        inv_rate_type,
        inv_rate_date,
        inv_exchange_rate,
        allow_inv_user_rate_type_flag,
        bill_another_project_flag,
        receiver_task_id,
        record_version_number,
        project_party_id,
        retention_level_code,
        bill_to_customer_id,
        ship_to_customer_id,
        default_top_task_cust_flag,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_project_customers') }}
),

transformed_exp_csf_pa_project_customers AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    project_id,
    customer_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    project_relationship_code,
    customer_bill_split,
    bill_to_address_id,
    ship_to_address_id,
    inv_currency_code,
    inv_rate_type,
    inv_rate_date,
    inv_exchange_rate,
    allow_inv_user_rate_type_flag,
    bill_another_project_flag,
    receiver_task_id,
    record_version_number,
    project_party_id,
    retention_level_code,
    bill_to_customer_id,
    ship_to_customer_id,
    default_top_task_cust_flag
    FROM source_stg_csf_pa_project_customers
),

final AS (
    SELECT
        project_id,
        customer_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        project_relationship_code,
        customer_bill_split,
        bill_to_address_id,
        ship_to_address_id,
        inv_currency_code,
        inv_rate_type,
        inv_rate_date,
        inv_exchange_rate,
        allow_inv_user_rate_type_flag,
        bill_another_project_flag,
        receiver_task_id,
        record_version_number,
        project_party_id,
        retention_level_code,
        bill_to_customer_id,
        ship_to_customer_id,
        default_top_task_cust_flag,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_project_customers
)

SELECT * FROM final
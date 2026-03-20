{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_xxgco_customer_mapp', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXGCO_CUSTOMER_MAPP',
        'target_table': 'STG_CG1_XXGCO_CUSTOMER_MAPP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.668887+00:00'
    }
) }}

WITH 

source_cg1_xxgco_customer_mappings AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        customer_mapping_id,
        person_id,
        cust_account_id,
        bill_to_site_use_id,
        ship_to_site_use_id,
        csr_email_address,
        start_date,
        end_date,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login
    FROM {{ source('raw', 'cg1_xxgco_customer_mappings') }}
),

source_stg_cg1_xxgco_customer_mapp AS (
    SELECT
        customer_mapping_id,
        person_id,
        cust_account_id,
        bill_to_site_use_id,
        ship_to_site_use_id,
        start_date,
        end_date,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        csr_email_address,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxgco_customer_mapp') }}
),

transformed_exp_cg1_xxgco_customer_mapp AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    customer_mapping_id,
    person_id,
    cust_account_id,
    bill_to_site_use_id,
    ship_to_site_use_id,
    csr_email_address,
    start_date,
    end_date,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login
    FROM source_stg_cg1_xxgco_customer_mapp
),

final AS (
    SELECT
        customer_mapping_id,
        person_id,
        cust_account_id,
        bill_to_site_use_id,
        ship_to_site_use_id,
        start_date,
        end_date,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        csr_email_address,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxgco_customer_mapp
)

SELECT * FROM final
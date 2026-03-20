{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dca_disti_accounts', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_DCA_DISTI_ACCOUNTS',
        'target_table': 'ST_DCA_DISTI_ACCOUNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.393612+00:00'
    }
) }}

WITH 

source_gg_dca_disti_accounts AS (
    SELECT
        account_id,
        source_profile_id,
        cra_disti_name,
        cust_account_no,
        bid_number,
        is_default,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        port_id,
        port_num,
        default_saf_interface,
        default_num_format,
        org_id,
        company_code,
        customer_id,
        global_name,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'gg_dca_disti_accounts') }}
),

transformed_exptrans AS (
    SELECT
    account_id,
    source_profile_id,
    cra_disti_name,
    cust_account_no,
    bid_number,
    is_default,
    active_flag,
    created_by,
    created_date,
    updated_by,
    last_update_date,
    port_id,
    port_num,
    default_saf_interface,
    default_num_format,
    org_id,
    company_code,
    customer_id,
    global_name,
    create_datetime,
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    1 AS batch_id,
    'I' AS action_code
    FROM source_gg_dca_disti_accounts
),

final AS (
    SELECT
        batch_id,
        account_id,
        source_profile_id,
        cra_disti_name,
        cust_account_no,
        bid_number,
        is_default,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        port_id,
        port_num,
        default_saf_interface,
        default_num_format,
        org_id,
        company_code,
        customer_id,
        global_name,
        create_datetime,
        action_code,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exptrans
)

SELECT * FROM final
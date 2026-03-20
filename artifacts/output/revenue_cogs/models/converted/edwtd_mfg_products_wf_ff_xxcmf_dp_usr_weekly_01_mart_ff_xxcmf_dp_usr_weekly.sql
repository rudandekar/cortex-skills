{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcmf_dp_usr_weekly', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_FF_XXCMF_DP_USR_WEEKLY',
        'target_table': 'FF_XXCMF_DP_USR_WEEKLY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.156681+00:00'
    }
) }}

WITH 

source_cg1_xxcmf_dp_usr_weekly AS (
    SELECT
        pid_code,
        customer_code,
        theater_code,
        sdate,
        usr_units_baseline_rev,
        usr_exceptions_rev,
        total_usr_rev,
        usr_units_baseline_nrev,
        usr_exceptions_nrev,
        total_usr_nrev,
        publish_date,
        etl_load_date,
        etl_source,
        naive_usr,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_xxcmf_dp_usr_weekly') }}
),

transformed_exp_xxcmf_dp_usr_weekly AS (
    SELECT
    pid_code,
    customer_code,
    theater_code,
    sdate,
    usr_units_baseline_rev,
    usr_exceptions_rev,
    total_usr_rev,
    usr_units_baseline_nrev,
    usr_exceptions_nrev,
    total_usr_nrev,
    publish_date,
    etl_load_date,
    etl_source,
    naive_usr,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cg1_xxcmf_dp_usr_weekly
),

final AS (
    SELECT
        pid_code,
        customer_code,
        theater_code,
        sdate,
        usr_units_baseline_rev,
        usr_exceptions_rev,
        total_usr_rev,
        usr_units_baseline_nrev,
        usr_exceptions_nrev,
        total_usr_nrev,
        publish_date,
        etl_load_date,
        etl_source,
        naive_usr,
        batch_id,
        create_datetime,
        action_code
    FROM transformed_exp_xxcmf_dp_usr_weekly
)

SELECT * FROM final
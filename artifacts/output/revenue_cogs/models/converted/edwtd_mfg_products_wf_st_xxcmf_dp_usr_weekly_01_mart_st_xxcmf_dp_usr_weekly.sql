{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_dp_usr_weekly', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_DP_USR_WEEKLY',
        'target_table': 'ST_XXCMF_DP_USR_WEEKLY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.868514+00:00'
    }
) }}

WITH 

source_ff_xxcmf_dp_usr_weekly AS (
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
    FROM {{ source('raw', 'ff_xxcmf_dp_usr_weekly') }}
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
    FROM source_ff_xxcmf_dp_usr_weekly
)

SELECT * FROM final
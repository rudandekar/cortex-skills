{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_specifics', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_SPECIFICS',
        'target_table': 'ST_XXCFI_CB_SPECIFICS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.763304+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_specifics AS (
    SELECT
        batch_id,
        specific_id,
        specific_name,
        description,
        blocked_flag,
        active_flag,
        deactivated_by,
        deactivated_dat,
        data_source_id,
        external_comments,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcfi_cb_specifics') }}
),

final AS (
    SELECT
        batch_id,
        specific_id,
        specific_name,
        description,
        blocked_flag,
        active_flag,
        deactivated_by,
        deactivated_dat,
        data_source_id,
        external_comments,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_specifics
)

SELECT * FROM final
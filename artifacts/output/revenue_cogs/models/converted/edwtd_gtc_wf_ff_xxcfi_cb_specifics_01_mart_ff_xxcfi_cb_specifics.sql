{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_specifics', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_SPECIFICS',
        'target_table': 'FF_XXCFI_CB_SPECIFICS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.597793+00:00'
    }
) }}

WITH 

source_xxcfi_cb_specifics AS (
    SELECT
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
        modified_date
    FROM {{ source('raw', 'xxcfi_cb_specifics') }}
),

transformed_exp_ff_xxcfi_cb_specifics AS (
    SELECT
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
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_specifics
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
    FROM transformed_exp_ff_xxcfi_cb_specifics
)

SELECT * FROM final
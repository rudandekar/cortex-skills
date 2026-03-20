{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_pos_batch', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_POS_BATCH',
        'target_table': 'EX_WIPS_BATCHES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.199996+00:00'
    }
) }}

WITH 

source_st_wips_batches AS (
    SELECT
        src_batch_id,
        batch_desc,
        batch_status,
        batch_posted_datetime,
        batch_type,
        source_profile_id,
        file_name,
        file_recieved_datetime,
        batch_due_date,
        active_flag,
        created_date,
        last_update_date,
        create_datetime,
        batch_id,
        action_code,
        duplicate_flag
    FROM {{ source('raw', 'st_wips_batches') }}
),

source_st_wips_batches AS (
    SELECT
        src_batch_id,
        batch_desc,
        batch_status,
        batch_posted_datetime,
        batch_type,
        source_profile_id,
        file_name,
        file_recieved_datetime,
        batch_due_date,
        active_flag,
        created_date,
        last_update_date,
        create_datetime,
        batch_id,
        action_code,
        duplicate_flag
    FROM {{ source('raw', 'st_wips_batches') }}
),

final AS (
    SELECT
        src_batch_id,
        batch_desc,
        batch_status,
        batch_posted_datetime,
        batch_type,
        source_profile_id,
        file_name,
        file_recieved_datetime,
        batch_due_date,
        active_flag,
        created_date,
        last_update_date,
        create_datetime,
        batch_id,
        action_code,
        exception_type,
        duplicate_flag
    FROM source_st_wips_batches
)

SELECT * FROM final
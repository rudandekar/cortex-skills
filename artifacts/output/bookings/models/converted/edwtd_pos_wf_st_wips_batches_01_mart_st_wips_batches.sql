{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wips_batches', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_BATCHES',
        'target_table': 'ST_WIPS_BATCHES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.797153+00:00'
    }
) }}

WITH 

source_ff_wips_batches AS (
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
    FROM {{ source('raw', 'ff_wips_batches') }}
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
        duplicate_flag
    FROM source_ff_wips_batches
)

SELECT * FROM final
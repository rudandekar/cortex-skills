{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wips_pos_trans_details', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_POS_TRANS_DETAILS',
        'target_table': 'ST_WIPS_POS_TRANS_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.351986+00:00'
    }
) }}

WITH 

source_gg_wips_pos_trans_details AS (
    SELECT
        detail_type,
        detail_value,
        pos_trans_id,
        detail_id,
        active_flag,
        serial_number_status,
        serial_number_reason_code,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        batch_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'gg_wips_pos_trans_details') }}
),

transformed_exptrans AS (
    SELECT
    detail_type,
    detail_value,
    pos_trans_id,
    detail_id,
    active_flag,
    serial_number_status,
    serial_number_reason_code,
    created_by,
    created_date,
    updated_by,
    last_update_date,
    create_datetime,
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    1 AS batch_id,
    'I' AS action_code
    FROM source_gg_wips_pos_trans_details
),

final AS (
    SELECT
        detail_type,
        detail_value,
        pos_trans_id,
        detail_id,
        active_flag,
        serial_number_status,
        serial_number_reason_code,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        create_datetime,
        batch_id,
        action_code,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exptrans
)

SELECT * FROM final
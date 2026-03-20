{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_pos_batch', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_POS_BATCH',
        'target_table': 'W_POS_BATCH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.543521+00:00'
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
        batch_id_int,
        batch_descr,
        batch_status_cd,
        batch_posted_dtm,
        batch_type_cd,
        bk_wips_originator_id_int,
        file_name,
        file_received_dtm,
        batch_due_dt,
        active_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type,
        batch_duplicate_flg
    FROM source_st_wips_batches
)

SELECT * FROM final
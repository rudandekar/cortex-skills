{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_batch', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_POS_BATCH',
        'target_table': 'N_POS_BATCH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.594886+00:00'
    }
) }}

WITH 

source_w_pos_batch AS (
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
    FROM {{ source('raw', 'w_pos_batch') }}
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
        batch_duplicate_flg
    FROM source_w_pos_batch
)

SELECT * FROM final
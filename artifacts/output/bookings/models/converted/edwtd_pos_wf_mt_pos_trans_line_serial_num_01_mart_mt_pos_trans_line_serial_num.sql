{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_pos_trans_line_serial_num', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_MT_POS_TRANS_LINE_SERIAL_NUM',
        'target_table': 'MT_POS_TRANS_LINE_SERIAL_NUM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.463629+00:00'
    }
) }}

WITH 

source_mt_pos_trans_line_serial_num AS (
    SELECT
        bk_pos_transaction_id_int,
        dv_scrubbed_serial_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_pos_trans_line_serial_num') }}
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
        dv_scrubbed_serial_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_pos_trans_line_serial_num
)

SELECT * FROM final
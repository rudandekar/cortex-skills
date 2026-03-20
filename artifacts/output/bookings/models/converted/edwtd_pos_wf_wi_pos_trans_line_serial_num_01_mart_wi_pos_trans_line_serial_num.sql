{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pos_trans_line_serial_num', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_POS_TRANS_LINE_SERIAL_NUM',
        'target_table': 'WI_POS_TRANS_LINE_SERIAL_NUM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.211013+00:00'
    }
) }}

WITH 

source_wi_pos_trans_line_serial_num AS (
    SELECT
        bk_pos_transaction_id_int,
        dv_scrubbed_serial_num,
        bk_detail_id_int,
        rnk
    FROM {{ source('raw', 'wi_pos_trans_line_serial_num') }}
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
        dv_scrubbed_serial_num,
        bk_detail_id_int,
        rnk
    FROM source_wi_pos_trans_line_serial_num
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_transaction_line_item', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_POS_TRANSACTION_LINE_ITEM',
        'target_table': 'N_POS_TRANSACTION_LINE_ITEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.702150+00:00'
    }
) }}

WITH 

source_w_pos_transaction_line_item AS (
    SELECT
        bk_pos_transaction_id_int,
        bk_detail_id_int,
        scrubbed_serial_number,
        reported_serial_number,
        rptd_ser_num_validity_flag,
        rptd_ser_num_validity_code,
        rptd_ser_num_reason_descr,
        part_serialized_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code
    FROM {{ source('raw', 'w_pos_transaction_line_item') }}
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
        bk_detail_id_int,
        scrubbed_serial_number,
        reported_serial_number,
        rptd_ser_num_validity_flag,
        rptd_ser_num_validity_code,
        rptd_ser_num_reason_descr,
        part_serialized_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_w_pos_transaction_line_item
)

SELECT * FROM final
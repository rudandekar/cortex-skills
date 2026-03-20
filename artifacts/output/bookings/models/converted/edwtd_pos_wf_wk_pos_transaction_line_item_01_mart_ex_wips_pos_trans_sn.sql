{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_pos_transaction_line_item', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WK_POS_TRANSACTION_LINE_ITEM',
        'target_table': 'EX_WIPS_POS_TRANS_SN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.935354+00:00'
    }
) }}

WITH 

source_st_wips_pos_trans_sn AS (
    SELECT
        pos_trans_id,
        detail_id,
        reported_serial_number,
        scrubbed_serial_number,
        part_serialized_yn,
        serial_number_status,
        serial_number_reason_code,
        valid_serial_number_yn,
        source_batch_id,
        returned_mfg_part_1,
        returned_mfg_part_2,
        returned_mfg_part_3,
        returned_billto_cust_id_1,
        returned_billto_cust_id_2,
        returned_billto_cust_id_3,
        returned_prod_family_1,
        returned_prod_family_2,
        returned_prod_family_3,
        interface_status,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        create_datetime,
        batch_id,
        action_code
    FROM {{ source('raw', 'st_wips_pos_trans_sn') }}
),

filtered_flt_ex_wips_pos_trans_sn AS (
    SELECT *
    FROM source_st_wips_pos_trans_sn
    WHERE ERROR_CHECK='NN'
),

transformed_exp_st_wips_pos_trans_sn AS (
    SELECT
    pos_trans_id,
    detail_id,
    reported_serial_number,
    scrubbed_serial_number,
    part_serialized_yn,
    serial_number_status,
    serial_number_reason_code,
    valid_serial_number_yn,
    action_code,
    edw_create_user,
    edw_update_user,
    edw_create_datetime,
    edw_update_datetime,
    IIF (ISNULL(SCRUBBED_SERIAL_NUMBER), 'N/A',SCRUBBED_SERIAL_NUMBER ) AS o_scrubbed_serial_number,
    IIF (ISNULL(PART_SERIALIZED_YN), 'N', PART_SERIALIZED_YN) AS o_part_serialized_yn,
    IIF (ISNULL(VALID_SERIAL_NUMBER_YN), 'N', VALID_SERIAL_NUMBER_YN) AS o_valid_serial_number,
    IIF (ISNULL(POS_TRANS_ID) OR ISNULL(DETAIL_ID), 'NN','NE') AS error_check
    FROM filtered_flt_ex_wips_pos_trans_sn
),

filtered_flt_w_pos_transaction_line_item AS (
    SELECT *
    FROM transformed_exp_st_wips_pos_trans_sn
    WHERE ERROR_CHECK='NE'
),

final AS (
    SELECT
        pos_trans_id,
        detail_id,
        reported_serial_number,
        scrubbed_serial_number,
        part_serialized_yn,
        serial_number_status,
        serial_number_reason_code,
        valid_serial_number_yn,
        source_batch_id,
        returned_mfg_part_1,
        returned_mfg_part_2,
        returned_mfg_part_3,
        returned_billto_cust_id_1,
        returned_billto_cust_id_2,
        returned_billto_cust_id_3,
        returned_prod_family_1,
        returned_prod_family_2,
        returned_prod_family_3,
        interface_status,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        create_datetime,
        batch_id,
        action_code,
        exception_type
    FROM filtered_flt_w_pos_transaction_line_item
)

SELECT * FROM final
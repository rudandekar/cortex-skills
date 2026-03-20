{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wips_pos_trans_sn', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_POS_TRANS_SN',
        'target_table': 'ST_WIPS_POS_TRANS_SN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.973698+00:00'
    }
) }}

WITH 

source_gg_wips_pos_trans_sn AS (
    SELECT
        pos_trans_id,
        detail_id,
        reported_serial_number,
        scrubbed_serial_number,
        part_serialized_yn,
        serial_number_status,
        serial_number_reason_code,
        valid_serial_number_yn,
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
        batch_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'gg_wips_pos_trans_sn') }}
),

transformed_exptrans AS (
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
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    'BatchId' AS batch_id,
    'I' AS action_code
    FROM source_gg_wips_pos_trans_sn
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
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exptrans
)

SELECT * FROM final
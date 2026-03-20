{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_dl_disti_fulf_kfk_f', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_DL_DISTI_FULF_KFK_F',
        'target_table': 'ST_INT_RAW_DL_DISTI_FULF_KFK_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.933034+00:00'
    }
) }}

WITH 

source_ff_st_int_raw_deal_disti_fulfillm_kafka AS (
    SELECT
        parent_id,
        revision_number,
        object_id,
        source_profile_id,
        dev_process_remark,
        next_action,
        start_date,
        deviation_id,
        dev_process_description,
        last_update_date,
        end_date,
        authorization_number,
        deviation_status,
        dev_process_status,
        dev_retry_count,
        linked_deviation_id,
        queue_object_id,
        deal_object_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'ff_st_int_raw_deal_disti_fulfillm_kafka') }}
),

final AS (
    SELECT
        parent_id,
        revision_number,
        object_id,
        source_profile_id,
        dev_process_remark,
        next_action,
        start_date,
        deviation_id,
        dev_process_description,
        last_update_date,
        end_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        authorization_number,
        dev_process_status,
        dev_retry_count,
        deviation_status,
        linked_deviation_id,
        queue_object_id,
        message_sequence_number,
        deal_object_id
    FROM source_ff_st_int_raw_deal_disti_fulfillm_kafka
)

SELECT * FROM final
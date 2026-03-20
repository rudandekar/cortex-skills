{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_raw_cq_dlnsterm_vw_kfk', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RAW_CQ_DLNSTERM_VW_KFK',
        'target_table': 'EL_INT_RAW_CQ_DLNSTERM_VW_KFK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.934468+00:00'
    }
) }}

WITH 

source_el_int_raw_cq_dlnsterm_vw_kfk AS (
    SELECT
        object_id,
        nsterm_id,
        nsterm_name,
        deal_object_id,
        created_date,
        created_by,
        updated_date,
        updated_by,
        as_subscription,
        as_transaction,
        as_fixed,
        tss_core,
        ros,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_raw_cq_dlnsterm_vw_kfk') }}
),

final AS (
    SELECT
        object_id,
        nsterm_id,
        nsterm_name,
        deal_object_id,
        created_date,
        created_by,
        updated_date,
        updated_by,
        as_subscription,
        as_transaction,
        as_fixed,
        tss_core,
        ros,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM source_el_int_raw_cq_dlnsterm_vw_kfk
)

SELECT * FROM final
{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_cq_dlnstr_vw_kfk_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_CQ_DLNSTR_VW_KFK_FF',
        'target_table': 'ST_INT_RAW_CQ_DLNSTR_VW_KFK_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.947811+00:00'
    }
) }}

WITH 

source_st_int_raw_cq_dealnsterm_vw_kafka AS (
    SELECT
        parent_id,
        nsterm_id,
        as_transaction,
        created_by,
        updated_by,
        ros,
        created_on,
        as_subscription,
        tss_core,
        updated_on,
        as_fixed,
        nsterm_name,
        object_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'st_int_raw_cq_dealnsterm_vw_kafka') }}
),

final AS (
    SELECT
        parent_id,
        nsterm_id,
        as_transaction,
        created_by,
        updated_by,
        ros,
        created_on,
        as_subscription,
        tss_core,
        updated_on,
        as_fixed,
        nsterm_name,
        object_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM source_st_int_raw_cq_dealnsterm_vw_kafka
)

SELECT * FROM final
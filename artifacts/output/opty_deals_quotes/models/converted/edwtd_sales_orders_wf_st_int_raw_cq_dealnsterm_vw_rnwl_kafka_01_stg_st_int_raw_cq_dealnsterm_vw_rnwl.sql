{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_cq_dealnsterm_vw_rnwl_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_CQ_DEALNSTERM_VW_RNWL_KAFKA',
        'target_table': 'ST_INT_RAW_CQ_DEALNSTERM_VW_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.981469+00:00'
    }
) }}

WITH 

source_el_int_raw_cq_dlnsterm_vw_rnwl_kfk1 AS (
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
        message_sequence_number
    FROM {{ source('raw', 'el_int_raw_cq_dlnsterm_vw_rnwl_kfk1') }}
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
        ros
    FROM source_el_int_raw_cq_dlnsterm_vw_rnwl_kfk1
)

SELECT * FROM final
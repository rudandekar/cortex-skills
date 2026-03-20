{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_deal_reopn_reason_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_DEAL_REOPN_REASON_KFKA',
        'target_table': 'EL_INT_DEAL_REOPN_REASON_KFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.927457+00:00'
    }
) }}

WITH 

source_el_int_deal_reopn_reason_kfka AS (
    SELECT
        deal_object_id,
        reopen_reason,
        created_by,
        created_on,
        updated_by,
        updated_on,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_deal_reopn_reason_kfka') }}
),

final AS (
    SELECT
        deal_object_id,
        reopen_reason,
        created_by,
        created_on,
        updated_by,
        updated_on,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM source_el_int_deal_reopn_reason_kfka
)

SELECT * FROM final
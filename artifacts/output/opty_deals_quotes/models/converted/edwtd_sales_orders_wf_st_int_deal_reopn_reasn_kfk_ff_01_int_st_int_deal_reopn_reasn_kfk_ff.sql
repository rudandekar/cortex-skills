{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_deal_reopn_reasn_kfk_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_DEAL_REOPN_REASN_KFK_FF',
        'target_table': 'ST_INT_DEAL_REOPN_REASN_KFK_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.968999+00:00'
    }
) }}

WITH 

source_st_int_deal_reopen_reason_kafka AS (
    SELECT
        parent_id,
        reopen_reason,
        updated_by,
        updated_on,
        created_by,
        created_on,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'st_int_deal_reopen_reason_kafka') }}
),

final AS (
    SELECT
        parent_id,
        reopen_reason,
        updated_by,
        updated_on,
        created_by,
        created_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number,
        edw_updated_date
    FROM source_st_int_deal_reopen_reason_kafka
)

SELECT * FROM final
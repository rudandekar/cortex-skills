{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_deal_quote_comment', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_DEAL_QUOTE_COMMENT',
        'target_table': 'ST_DEAL_QUOTE_COMMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.943025+00:00'
    }
) }}

WITH 

source_st_deal_quote_comments AS (
    SELECT
        deal_object_id,
        deal_id,
        quote_id,
        updated_on,
        created_by,
        created_on,
        is_new,
        created_on_as_date,
        note_id,
        reject_reason_flg,
        parnter_cmmnt,
        notes,
        quote_obj_id,
        addressed_to,
        message_sequence_number,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm
    FROM {{ source('raw', 'st_deal_quote_comments') }}
),

final AS (
    SELECT
        deal_object_id,
        deal_id,
        quote_id,
        updated_on,
        created_by,
        created_on,
        is_new,
        created_on_as_date,
        note_id,
        reject_reason_flg,
        parnter_cmmnt,
        notes,
        quote_obj_id,
        addressed_to,
        message_sequence_number,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm
    FROM source_st_deal_quote_comments
)

SELECT * FROM final
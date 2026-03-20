{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cq_deal_contact_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CQ_DEAL_CONTACT_KAFKA',
        'target_table': 'st_cq_deal_contact',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.983094+00:00'
    }
) }}

WITH 

source_el_cq_deal_contact_kafka AS (
    SELECT
        batch_id,
        deal_object_id,
        object_id,
        contact_id,
        contact_name,
        contact_type,
        created_on,
        created_by,
        updated_on,
        updated_by,
        contact_row,
        dm_update_date,
        contact_postn_id,
        siebel_postn_id,
        create_datetime,
        action_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_user,
        edw_create_dtm,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_cq_deal_contact_kafka') }}
),

final AS (
    SELECT
        batch_id,
        deal_object_id,
        object_id,
        contact_id,
        contact_name,
        contact_type,
        created_on,
        created_by,
        updated_on,
        updated_by,
        contact_row,
        dm_update_date,
        contact_postn_id,
        siebel_postn_id,
        create_datetime,
        action_code
    FROM source_el_cq_deal_contact_kafka
)

SELECT * FROM final
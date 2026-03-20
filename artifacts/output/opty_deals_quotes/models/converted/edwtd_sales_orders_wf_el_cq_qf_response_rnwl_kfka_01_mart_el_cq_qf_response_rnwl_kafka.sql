{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cq_qf_response_rnwl_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_CQ_QF_RESPONSE_RNWL_KFKA',
        'target_table': 'EL_CQ_QF_RESPONSE_RNWL_KAFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.891631+00:00'
    }
) }}

WITH 

source_el_cq_qf_response_rnwl_kafka AS (
    SELECT
        batch_id,
        deal_object_id,
        question_id,
        value_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_cq_qf_response_rnwl_kafka') }}
),

final AS (
    SELECT
        batch_id,
        deal_object_id,
        question_id,
        value_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_el_cq_qf_response_rnwl_kafka
)

SELECT * FROM final
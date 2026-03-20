{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cq_competitor_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_CQ_COMPETITOR_KFKA',
        'target_table': 'EL_CQ_COMPETITOR_KAFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.923582+00:00'
    }
) }}

WITH 

source_el_cq_competitor_kafka AS (
    SELECT
        batch_id,
        object_id,
        deal_object_id,
        competitor_name,
        comp_prod_name,
        comp_type,
        comp_tech,
        comp_add_info,
        created_by,
        created_on,
        updated_by,
        updated_on,
        dm_update_date,
        source,
        initiated_source,
        comp_prod_eu_price,
        create_datetime,
        action_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_cq_competitor_kafka') }}
),

final AS (
    SELECT
        batch_id,
        object_id,
        deal_object_id,
        competitor_name,
        comp_prod_name,
        comp_type,
        comp_tech,
        comp_add_info,
        created_by,
        created_on,
        updated_by,
        updated_on,
        dm_update_date,
        source,
        initiated_source,
        comp_prod_eu_price,
        create_datetime,
        action_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM source_el_cq_competitor_kafka
)

SELECT * FROM final
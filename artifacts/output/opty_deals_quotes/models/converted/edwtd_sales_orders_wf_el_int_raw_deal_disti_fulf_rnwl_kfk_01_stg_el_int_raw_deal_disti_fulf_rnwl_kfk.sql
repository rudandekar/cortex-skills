{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_raw_deal_disti_fulf_rnwl_kfk', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RAW_DEAL_DISTI_FULF_RNWL_KFK',
        'target_table': 'EL_INT_RAW_DEAL_DISTI_FULF_RNWL_KFK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.949800+00:00'
    }
) }}

WITH 

source_el_int_raw_deal_disti_fulf_rnwl_kfk AS (
    SELECT
        object_id,
        deal_object_id,
        quote_object_id,
        source_profile_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_user,
        edw_update_dtm,
        ref_entity_number,
        quote_status_id
    FROM {{ source('raw', 'el_int_raw_deal_disti_fulf_rnwl_kfk') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        quote_object_id,
        source_profile_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_user,
        edw_update_dtm,
        ref_entity_number,
        quote_status_id
    FROM source_el_int_raw_deal_disti_fulf_rnwl_kfk
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_deal_quote_rnwl_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_DEAL_QUOTE_RNWL_KAFKA',
        'target_table': 'EL_DEAL_QUOTE_RNWL_KFK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.932582+00:00'
    }
) }}

WITH 

source_el_deal_quote_rnwl_kfk AS (
    SELECT
        deal_object_id,
        deal_id,
        quote_id,
        quote_status,
        intended_use,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm
    FROM {{ source('raw', 'el_deal_quote_rnwl_kfk') }}
),

final AS (
    SELECT
        deal_object_id,
        deal_id,
        quote_id,
        quote_status,
        intended_use,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm
    FROM source_el_deal_quote_rnwl_kfk
)

SELECT * FROM final
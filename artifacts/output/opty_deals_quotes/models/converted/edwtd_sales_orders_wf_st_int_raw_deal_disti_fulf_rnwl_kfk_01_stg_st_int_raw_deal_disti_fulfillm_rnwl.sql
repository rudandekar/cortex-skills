{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_deal_disti_fulf_rnwl_kfk', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_DEAL_DISTI_FULF_RNWL_KFK',
        'target_table': 'ST_INT_RAW_DEAL_DISTI_FULFILLM_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.900331+00:00'
    }
) }}

WITH 

source_st_int_raw_deal_disti_fulfillment_rnwl AS (
    SELECT
        object_id,
        deal_object_id,
        authorization_number,
        deviation_id,
        deviation_status,
        dev_process_description,
        dev_process_remark,
        dev_process_status,
        dev_retry_count,
        end_date,
        last_update_date,
        linked_deviation_id,
        next_action,
        queue_object_id,
        quote_object_id,
        revision_number,
        source_profile_id,
        start_date
    FROM {{ source('raw', 'st_int_raw_deal_disti_fulfillment_rnwl') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        authorization_number,
        deviation_id,
        deviation_status,
        dev_process_description,
        dev_process_remark,
        dev_process_status,
        dev_retry_count,
        end_date,
        last_update_date,
        linked_deviation_id,
        next_action,
        queue_object_id,
        quote_object_id,
        revision_number,
        source_profile_id,
        start_date
    FROM source_st_int_raw_deal_disti_fulfillment_rnwl
)

SELECT * FROM final
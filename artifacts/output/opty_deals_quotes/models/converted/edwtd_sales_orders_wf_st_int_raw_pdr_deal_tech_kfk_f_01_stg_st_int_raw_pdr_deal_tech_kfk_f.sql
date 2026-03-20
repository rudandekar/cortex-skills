{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_pdr_deal_tech_kfk_f', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_PDR_DEAL_TECH_KFK_F',
        'target_table': 'ST_INT_RAW_PDR_DEAL_TECH_KFK_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.921422+00:00'
    }
) }}

WITH 

source_ff_st_int_raw_pdr_deal_tech_kafka AS (
    SELECT
        parent_id,
        object_id,
        deal_object_id,
        percentage_of_mix,
        technology,
        created_by,
        created_on,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'ff_st_int_raw_pdr_deal_tech_kafka') }}
),

final AS (
    SELECT
        parent_id,
        object_id,
        deal_object_id,
        percentage_of_mix,
        technology,
        created_by,
        created_on,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM source_ff_st_int_raw_pdr_deal_tech_kafka
)

SELECT * FROM final
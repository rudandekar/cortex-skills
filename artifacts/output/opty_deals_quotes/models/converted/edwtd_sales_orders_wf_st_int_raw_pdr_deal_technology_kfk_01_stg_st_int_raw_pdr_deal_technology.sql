{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_pdr_deal_technology_kfk', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_PDR_DEAL_TECHNOLOGY_KFK',
        'target_table': 'ST_INT_RAW_PDR_DEAL_TECHNOLOGY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.916220+00:00'
    }
) }}

WITH 

source_el_int_raw_pdr_deal_tech_kfk AS (
    SELECT
        object_id,
        deal_object_id,
        created_by,
        created_on,
        percentage_of_mix,
        technology,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        message_sequence_number
    FROM {{ source('raw', 'el_int_raw_pdr_deal_tech_kfk') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        technology,
        percentage_of_mix,
        created_by,
        created_on,
        updated_by,
        updated_on
    FROM source_el_int_raw_pdr_deal_tech_kfk
)

SELECT * FROM final
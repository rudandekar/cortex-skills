{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_cq_dl_prty_vw_kfk_f', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_CQ_DL_PRTY_VW_KFK_F',
        'target_table': 'ST_INT_RAW_CQ_DL_PRTY_VW_KFK_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.950600+00:00'
    }
) }}

WITH 

source_ff_st_int_raw_cq_deal_party_vw_kafka AS (
    SELECT
        parent_id,
        party_name,
        party_type_name,
        party_id,
        site_id,
        object_id,
        party_type_level,
        primary_flag,
        created_by,
        created_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'ff_st_int_raw_cq_deal_party_vw_kafka') }}
),

final AS (
    SELECT
        parent_id,
        party_name,
        party_type_name,
        party_id,
        site_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        object_id,
        party_type_level,
        primary_flag,
        message_sequence_number,
        created_by,
        created_date
    FROM source_ff_st_int_raw_cq_deal_party_vw_kafka
)

SELECT * FROM final
{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_cq_dl_prty_vw_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_CQ_DL_PRTY_VW_KFKA',
        'target_table': 'ST_INT_RAW_CQ_DEAL_PARTY_VW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.865145+00:00'
    }
) }}

WITH 

source_el_int_raw_cq_dl_prty_vw_kfka AS (
    SELECT
        object_id,
        deal_object_id,
        party_name,
        party_type_name,
        primary_flag,
        site_id,
        party_id,
        party_type_level,
        created_date,
        created_by,
        updated_date,
        updated_by,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        message_sequence_number
    FROM {{ source('raw', 'el_int_raw_cq_dl_prty_vw_kfka') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        party_name,
        party_type_name,
        primary_flag,
        site_id,
        party_id,
        party_type_level,
        created_date,
        created_by,
        updated_date,
        updated_by
    FROM source_el_int_raw_cq_dl_prty_vw_kfka
)

SELECT * FROM final
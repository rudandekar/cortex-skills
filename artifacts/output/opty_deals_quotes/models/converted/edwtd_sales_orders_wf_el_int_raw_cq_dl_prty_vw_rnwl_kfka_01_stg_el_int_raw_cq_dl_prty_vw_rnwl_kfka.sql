{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_raw_cq_dl_prty_vw_rnwl_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RAW_CQ_DL_PRTY_VW_RNWL_KFKA',
        'target_table': 'EL_INT_RAW_CQ_DL_PRTY_VW_RNWL_KFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.879749+00:00'
    }
) }}

WITH 

source_el_int_raw_cq_dl_prty_vw_rnwl_kfka AS (
    SELECT
        deal_object_id,
        object_id,
        bill_to_begeo_id,
        bill_to_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_raw_cq_dl_prty_vw_rnwl_kfka') }}
),

final AS (
    SELECT
        deal_object_id,
        object_id,
        bill_to_begeo_id,
        bill_to_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_el_int_raw_cq_dl_prty_vw_rnwl_kfka
)

SELECT * FROM final
{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_cq_sngl_quote_vw_rnwl_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_CQ_SNGL_QUOTE_VW_RNWL_KAFKA',
        'target_table': 'EL_INT_CQ_SNGL_QUOTE_VW_RNWL_KFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.916910+00:00'
    }
) }}

WITH 

source_el_int_cq_sngl_quote_vw_rnwl_kfka AS (
    SELECT
        object_id,
        deal_object_id,
        created_by,
        created_on,
        updated_by,
        updated_on,
        active,
        price_list_id,
        quote_id,
        quote_name,
        is_q2o_eligible,
        is_disti_q2o_eligible,
        fulfillment_type,
        intended_use,
        is_stringent,
        is_finalized,
        federal_flg,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_user,
        edw_update_dtm,
        quote_status_id,
        ref_entity_number,
        currency_code,
        buy_method,
        is_pcm
    FROM {{ source('raw', 'el_int_cq_sngl_quote_vw_rnwl_kfka') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        created_by,
        created_on,
        updated_by,
        updated_on,
        active,
        price_list_id,
        quote_id,
        quote_name,
        is_q2o_eligible,
        is_disti_q2o_eligible,
        fulfillment_type,
        intended_use,
        is_stringent,
        is_finalized,
        federal_flg,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_user,
        edw_update_dtm,
        quote_status_id,
        ref_entity_number,
        currency_code,
        buy_method,
        is_pcm
    FROM source_el_int_cq_sngl_quote_vw_rnwl_kfka
)

SELECT * FROM final
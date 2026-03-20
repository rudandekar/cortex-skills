{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_cq_state_trnstn_int_vw_rnwl_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_CQ_STATE_TRNSTN_INT_VW_RNWL_KFKA',
        'target_table': 'EL_CQ_STATE_TRNSTN_INT_VW_RNWL_KFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.889329+00:00'
    }
) }}

WITH 

source_el_cq_state_trnstn_int_vw_rnwl_kfka AS (
    SELECT
        deal_object_id,
        object_id,
        state,
        transition_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_cq_state_trnstn_int_vw_rnwl_kfka') }}
),

final AS (
    SELECT
        deal_object_id,
        object_id,
        state,
        transition_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        edw_update_dtm,
        edw_update_user
    FROM source_el_cq_state_trnstn_int_vw_rnwl_kfka
)

SELECT * FROM final
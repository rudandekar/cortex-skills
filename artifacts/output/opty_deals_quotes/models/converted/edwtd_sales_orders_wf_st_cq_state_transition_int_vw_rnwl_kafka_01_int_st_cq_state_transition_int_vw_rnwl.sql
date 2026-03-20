{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_cq_state_transition_int_vw_rnwl_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CQ_STATE_TRANSITION_INT_VW_RNWL_KAFKA',
        'target_table': 'ST_CQ_STATE_TRANSITION_INT_VW_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.938021+00:00'
    }
) }}

WITH 

source_el_cq_state_trnstn_int_vw_rnwl_kfka1 AS (
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
    FROM {{ source('raw', 'el_cq_state_trnstn_int_vw_rnwl_kfka1') }}
),

final AS (
    SELECT
        deal_object_id,
        object_id,
        state,
        transition_date,
        transition_initiated_by,
        source,
        mdm_deal_status,
        pdr_deal_status,
        created_by,
        created_on
    FROM source_el_cq_state_trnstn_int_vw_rnwl_kfka1
)

SELECT * FROM final
{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_cq_state_trans_int_vw_kfk_ff', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CQ_STATE_TRANS_INT_VW_KFK_FF',
        'target_table': 'ST_CQ_STATE_TRN_INT_VW_KFK_FF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.907028+00:00'
    }
) }}

WITH 

source_st_cq_state_transition_int_vw_kfk AS (
    SELECT
        parent_id,
        object_id,
        state,
        transition_date,
        transition_initiated_by,
        created_by,
        created_on,
        source,
        mdm_deal_status,
        pdr_deal_status,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'st_cq_state_transition_int_vw_kfk') }}
),

final AS (
    SELECT
        parent_id,
        object_id,
        state,
        transition_date,
        transition_initiated_by,
        created_by,
        created_on,
        source,
        mdm_deal_status,
        pdr_deal_status,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM source_st_cq_state_transition_int_vw_kfk
)

SELECT * FROM final
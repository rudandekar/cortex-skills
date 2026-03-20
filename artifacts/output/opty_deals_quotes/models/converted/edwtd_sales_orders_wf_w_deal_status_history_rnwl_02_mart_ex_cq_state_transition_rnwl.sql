{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_status_history_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_STATUS_HISTORY_RNWL',
        'target_table': 'EX_CQ_STATE_TRANSITION_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.910868+00:00'
    }
) }}

WITH 

source_w_deal_status_history_rnwl AS (
    SELECT
        bk_deal_status_cd,
        bk_deal_id,
        bk_status_trx_dtm,
        bk_deal_status_ss_cd,
        dv_status_trx_dt,
        cisco_worker_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_status_history_rnwl') }}
),

source_ex_cq_state_transition_rnwl AS (
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
        created_on,
        exception_type
    FROM {{ source('raw', 'ex_cq_state_transition_rnwl') }}
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
        created_on,
        exception_type
    FROM source_ex_cq_state_transition_rnwl
)

SELECT * FROM final
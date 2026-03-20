{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_claim_event', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CLAIM_EVENT',
        'target_table': 'EX_XXNCE_CLAIM_EVENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.036761+00:00'
    }
) }}

WITH 

source_st_xxnce_claim_event AS (
    SELECT
        event_id,
        event_type,
        creation_date,
        created_by,
        last_updated_by,
        last_updated_date,
        object_version_number,
        claim_id,
        pending_action_actor,
        actor_type,
        claim_trx_sc_id,
        claim_status,
        trxn_approval_status
    FROM {{ source('raw', 'st_xxnce_claim_event') }}
),

source_w_claim_event AS (
    SELECT
        claim_event_key,
        sk_claim_event_id,
        ce_actn_pndg_csco_wrkr_pty_key,
        claim_key,
        claim_event_type_descr,
        create_dtm,
        last_update_dtm,
        sk_trx_sales_credit_id,
        claim_event_status_name,
        claim_event_trx_status_name,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_claim_event') }}
),

transformed_exp_xxnce_claim_event AS (
    SELECT
    event_id,
    event_type,
    creation_date,
    created_by,
    last_updated_by,
    last_updated_date,
    object_version_number,
    claim_id,
    pending_action_actor,
    actor_type,
    claim_trx_sc_id,
    claim_status,
    trxn_approval_status,
    'RI' AS exception_type
    FROM source_w_claim_event
),

final AS (
    SELECT
        event_id,
        event_type,
        creation_date,
        created_by,
        last_updated_by,
        last_updated_date,
        object_version_number,
        claim_id,
        pending_action_actor,
        actor_type,
        claim_trx_sc_id,
        claim_status,
        trxn_approval_status,
        exception_type
    FROM transformed_exp_xxnce_claim_event
)

SELECT * FROM final
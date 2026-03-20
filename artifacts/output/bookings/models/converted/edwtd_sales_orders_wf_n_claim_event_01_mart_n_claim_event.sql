{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_claim_event', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CLAIM_EVENT',
        'target_table': 'N_CLAIM_EVENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.429815+00:00'
    }
) }}

WITH 

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

final AS (
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
        edw_update_user
    FROM source_w_claim_event
)

SELECT * FROM final
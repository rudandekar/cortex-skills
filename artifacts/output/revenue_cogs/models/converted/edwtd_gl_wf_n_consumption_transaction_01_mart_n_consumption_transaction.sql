{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_consumption_transaction', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_CONSUMPTION_TRANSACTION',
        'target_table': 'N_CONSUMPTION_TRANSACTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.871522+00:00'
    }
) }}

WITH 

source_n_consumption_transaction AS (
    SELECT
        consumption_transaction_key,
        material_transaction_key,
        po_shipment_distribution_key,
        previous_owner_invnty_org_key,
        item_key,
        accrual_gnrl_ledger_acct_key,
        new_owner_invnty_org_key,
        created_by_csco_wrkr_pty_key,
        create_dt,
        last_update_dt,
        consumption_net_qty,
        blanket_usd_price,
        consumption_processed_flg,
        source_reported_po_num,
        transaction_dt,
        sk_transaction_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_consumption_transaction') }}
),

final AS (
    SELECT
        consumption_transaction_key,
        material_transaction_key,
        po_shipment_distribution_key,
        previous_owner_invnty_org_key,
        item_key,
        accrual_gnrl_ledger_acct_key,
        new_owner_invnty_org_key,
        created_by_csco_wrkr_pty_key,
        create_dt,
        last_update_dt,
        consumption_net_qty,
        blanket_usd_price,
        consumption_processed_flg,
        source_reported_po_num,
        transaction_dt,
        sk_transaction_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_consumption_transaction
)

SELECT * FROM final
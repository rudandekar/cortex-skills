{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_consumption_transaction', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_CONSUMPTION_TRANSACTION',
        'target_table': 'W_CONSUMPTION_TRANSACTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.684323+00:00'
    }
) }}

WITH 

source_ex_mtl_consumption_trx AS (
    SELECT
        transaction_id,
        consumption_release_id,
        consumption_po_header_id,
        consumption_processed_flag,
        request_id,
        created_by,
        program_application_id,
        creation_date,
        program_id,
        last_updated_by,
        program_update_date,
        last_update_date,
        last_update_login,
        net_qty,
        batch_id,
        rate,
        rate_type,
        tax_code_id,
        tax_rate,
        error_code,
        recoverable_tax,
        non_recoverable_tax,
        tax_recovery_rate,
        parent_transaction_id,
        charge_account_id,
        variance_account_id,
        global_agreement_flag,
        need_by_date,
        error_explanation,
        secondary_net_qty,
        blanket_price,
        po_distribution_id,
        interface_distribution_ref,
        transaction_source_id,
        inventory_item_id,
        accrual_account_id,
        organization_id,
        owning_organization_id,
        transaction_date,
        global_name,
        create_datetime,
        action_cd,
        source_commit_time,
        refresh_datetime,
        exception_type
    FROM {{ source('raw', 'ex_mtl_consumption_trx') }}
),

source_w_consumption_transaction AS (
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
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_consumption_transaction') }}
),

source_sm_consumption_transaction AS (
    SELECT
        consumption_transaction_key,
        sk_transaction_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_consumption_transaction') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_sm_consumption_transaction
)

SELECT * FROM final
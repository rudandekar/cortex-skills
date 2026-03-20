{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_ar_trx_2tier_invoice', 'batch', 'edwtd_2tier'],
    meta={
        'source_workflow': 'wf_m_SM_AR_TRX_2TIER_INVOICE',
        'target_table': 'SM_AR_TRX_2TIER_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.233917+00:00'
    }
) }}

WITH 

source_st_om_cfi_2tier_trx_all AS (
    SELECT
        amount,
        created_by,
        creation_date,
        customer_id,
        customer_trx_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        latest_flag,
        org_id,
        period_num,
        period_year,
        quantity_invoiced,
        set_of_books_id,
        transaction_id,
        trx_number,
        trx_type,
        batch_id,
        create_datetime,
        action_cd
    FROM {{ source('raw', 'st_om_cfi_2tier_trx_all') }}
),

final AS (
    SELECT
        ar_trx_2tier_invoice_key,
        sk_transaction_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM source_st_om_cfi_2tier_trx_all
)

SELECT * FROM final
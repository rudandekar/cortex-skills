{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_iby_payments_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_IBY_PAYMENTS_ALL',
        'target_table': 'EL_IBY_PAYMENTS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.988233+00:00'
    }
) }}

WITH 

source_st_mf_iby_payments_all AS (
    SELECT
        batch_id,
        payment_id,
        payment_method_code,
        payment_status,
        payment_amount,
        payment_currency_code,
        internal_bank_account_id,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        payment_date,
        external_bank_account_id,
        global_name,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_iby_payments_all') }}
),

final AS (
    SELECT
        payment_id,
        payment_method_code,
        payment_status,
        payment_amount,
        payment_currency_code,
        internal_bank_account_id,
        org_id,
        creation_date,
        last_update_date,
        payment_date,
        external_bank_account_id,
        global_name,
        ges_update_date,
        create_datetime
    FROM source_st_mf_iby_payments_all
)

SELECT * FROM final
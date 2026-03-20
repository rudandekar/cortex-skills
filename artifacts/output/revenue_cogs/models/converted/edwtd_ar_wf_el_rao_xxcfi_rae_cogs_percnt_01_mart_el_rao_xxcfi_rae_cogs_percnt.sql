{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_rao_xxcfi_rae_cogs_percnt', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_RAO_XXCFI_RAE_COGS_PERCNT',
        'target_table': 'EL_RAO_XXCFI_RAE_COGS_PERCNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.324332+00:00'
    }
) }}

WITH 

source_el_rao_xxcfi_rae_cogs_percnt AS (
    SELECT
        adjustment_line_id,
        global_name,
        adjustment_accounting_id,
        adjustment_id,
        batch_source,
        code_combination_id,
        cogs_deferral_flag,
        cogs_rev_gl_date,
        cogs_rev_posted_date,
        currency_code,
        customer_trx_line_id,
        dist_bal_type,
        distribution_type,
        dv_amount,
        dv_percent,
        event_id,
        gl_date,
        gl_posted_date,
        gl_posted_flag,
        ledger_id,
        line_id,
        nrs_transition_flg,
        oa_sku_type,
        org_id,
        rol_transaction_id,
        source_batch_name,
        source_data_key5,
        meraki_flag,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'el_rao_xxcfi_rae_cogs_percnt') }}
),

final AS (
    SELECT
        adjustment_line_id,
        global_name,
        adjustment_accounting_id,
        adjustment_id,
        batch_source,
        code_combination_id,
        cogs_deferral_flag,
        cogs_rev_gl_date,
        cogs_rev_posted_date,
        currency_code,
        customer_trx_line_id,
        dist_bal_type,
        distribution_type,
        dv_amount,
        dv_percent,
        event_id,
        gl_date,
        gl_posted_date,
        gl_posted_flag,
        ledger_id,
        line_id,
        nrs_transition_flg,
        oa_sku_type,
        org_id,
        rol_transaction_id,
        source_batch_name,
        source_data_key5,
        meraki_flag,
        edw_create_dtm,
        edw_update_dtm
    FROM source_el_rao_xxcfi_rae_cogs_percnt
)

SELECT * FROM final
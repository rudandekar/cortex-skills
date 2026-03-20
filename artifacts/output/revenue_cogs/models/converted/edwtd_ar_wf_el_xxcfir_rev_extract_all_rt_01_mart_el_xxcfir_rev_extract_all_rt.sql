{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfir_rev_extract_all_rt', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFIR_REV_EXTRACT_ALL_RT',
        'target_table': 'EL_XXCFIR_REV_EXTRACT_ALL_RT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.495121+00:00'
    }
) }}

WITH 

source_el_xxcfir_rev_extract_all_rt AS (
    SELECT
        transaction_id,
        application_name,
        application_id,
        ledger_id,
        org_id,
        customer_trx_id,
        trx_integer,
        trx_date,
        trx_line_id,
        element_type,
        rule_start_date,
        rule_end_date,
        accounting_rule_duration,
        attribute9,
        attribute10,
        attribute14,
        attribute15,
        accounting_rule_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        attribute21,
        attribute22
    FROM {{ source('raw', 'el_xxcfir_rev_extract_all_rt') }}
),

final AS (
    SELECT
        transaction_id,
        application_name,
        application_id,
        ledger_id,
        org_id,
        customer_trx_id,
        trx_integer,
        trx_date,
        trx_line_id,
        element_type,
        rule_start_date,
        rule_end_date,
        accounting_rule_duration,
        attribute9,
        attribute10,
        attribute14,
        attribute15,
        accounting_rule_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        attribute21,
        attribute22
    FROM source_el_xxcfir_rev_extract_all_rt
)

SELECT * FROM final
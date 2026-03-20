{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfir_rev_ext_sub_vip', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFIR_REV_EXT_SUB_VIP',
        'target_table': 'EL_XXCFIR_REV_EXT_SUB_VIP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.298152+00:00'
    }
) }}

WITH 

source_el_xxcfir_rev_ext_sub_rma AS (
    SELECT
        transaction_id,
        application_name,
        application_id,
        ledger_id,
        org_id,
        trx_class,
        order_line_id,
        trx_line_id,
        amount,
        order_quantity,
        currency_code,
        process_status,
        element_type,
        rule_start_date,
        rule_end_date,
        accounting_rule_duration,
        attribute1,
        attribute2,
        attribute6,
        sku,
        accounting_rule_name,
        transaction_source,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        attribute12,
        attribute13
    FROM {{ source('raw', 'el_xxcfir_rev_ext_sub_rma') }}
),

final AS (
    SELECT
        transaction_id,
        application_name,
        application_id,
        ledger_id,
        org_id,
        trx_class,
        order_line_id,
        trx_line_id,
        amount,
        order_quantity,
        currency_code,
        process_status,
        element_type,
        rule_start_date,
        rule_end_date,
        accounting_rule_duration,
        attribute1,
        attribute2,
        attribute6,
        sku,
        accounting_rule_name,
        transaction_source,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        attribute12,
        attribute13,
        attribute4,
        attribute16,
        attribute21,
        attribute24,
        attribute8
    FROM source_el_xxcfir_rev_ext_sub_rma
)

SELECT * FROM final
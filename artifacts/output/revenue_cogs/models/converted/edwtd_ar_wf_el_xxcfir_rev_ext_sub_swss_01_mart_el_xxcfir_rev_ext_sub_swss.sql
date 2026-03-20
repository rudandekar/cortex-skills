{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfir_rev_ext_sub_swss', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFIR_REV_EXT_SUB_SWSS',
        'target_table': 'EL_XXCFIR_REV_EXT_SUB_SWSS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.294024+00:00'
    }
) }}

WITH 

source_el_xxcfir_rev_ext_sub_swss AS (
    SELECT
        transaction_id,
        application_name,
        application_id,
        ledger_id,
        org_id,
        trx_class,
        amount,
        order_quantity,
        currency_code,
        element_type,
        rule_start_date,
        rule_end_date,
        accounting_rule_duration,
        attribute1,
        attribute2,
        sku,
        accounting_rule_name,
        transaction_source,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM {{ source('raw', 'el_xxcfir_rev_ext_sub_swss') }}
),

final AS (
    SELECT
        transaction_id,
        application_name,
        application_id,
        ledger_id,
        org_id,
        trx_class,
        amount,
        order_quantity,
        currency_code,
        element_type,
        rule_start_date,
        rule_end_date,
        accounting_rule_duration,
        attribute1,
        attribute2,
        sku,
        accounting_rule_name,
        transaction_source,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM source_el_xxcfir_rev_ext_sub_swss
)

SELECT * FROM final
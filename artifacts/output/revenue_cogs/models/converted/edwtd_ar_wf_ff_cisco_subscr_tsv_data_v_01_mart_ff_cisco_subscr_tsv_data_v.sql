{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cisco_subscr_tsv_data_v', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_FF_CISCO_SUBSCR_TSV_DATA_V',
        'target_table': 'FF_CISCO_SUBSCR_TSV_DATA_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.841807+00:00'
    }
) }}

WITH 

source_cisco_subscr_tsv_data_v AS (
    SELECT
        subscription_ref_id,
        subscription_id,
        status,
        initial_term,
        renewal_term,
        prepayment_term,
        renewal_counter,
        bdom,
        term_start_date,
        term_end_date,
        offer_type,
        billing_model,
        line_status,
        last_order_type,
        currency_code,
        product_id,
        tsv_amount,
        creation_date,
        last_modified_date,
        cancel_date,
        charge_type,
        offer_code,
        unbilled_amount,
        flexup_sku
    FROM {{ source('raw', 'cisco_subscr_tsv_data_v') }}
),

final AS (
    SELECT
        subscription_ref_id,
        subscription_id,
        status,
        initial_term,
        renewal_term,
        prepayment_term,
        renewal_counter,
        bdom,
        term_start_date,
        term_end_date,
        offer_type,
        billing_model,
        line_status,
        last_order_type,
        currency_code,
        product_id,
        tsv_amount,
        creation_date,
        last_modified_date,
        cancel_date,
        charge_type,
        offer_code,
        unbilled_amount,
        flexup_sku
    FROM source_cisco_subscr_tsv_data_v
)

SELECT * FROM final
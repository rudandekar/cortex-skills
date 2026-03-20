{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_recurring_offer_revenue_tss_def', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_RECURRING_OFFER_REVENUE_TSS_DEF',
        'target_table': 'WI_RECUR_OFFER_KEYS_TSS_DEF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.034545+00:00'
    }
) }}

WITH 

source_wi_recur_offer_keys_tss_def AS (
    SELECT
        dv_recurring_offer_cd,
        product_key,
        dv_product_key,
        sales_order_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_recur_offer_keys_tss_def') }}
),

final AS (
    SELECT
        dv_recurring_offer_cd,
        product_key,
        dv_product_key,
        sales_order_key,
        sk_offer_attribution_id_int
    FROM source_wi_recur_offer_keys_tss_def
)

SELECT * FROM final
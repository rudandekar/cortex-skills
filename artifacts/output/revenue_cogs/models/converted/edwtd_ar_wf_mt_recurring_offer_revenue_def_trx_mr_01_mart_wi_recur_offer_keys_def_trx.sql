{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_recurring_offer_revenue_def_trx_mr', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_MT_RECURRING_OFFER_REVENUE_DEF_TRX_MR',
        'target_table': 'WI_RECUR_OFFER_KEYS_DEF_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.194508+00:00'
    }
) }}

WITH 

source_wi_recur_offer_keys_def_trx AS (
    SELECT
        dv_recurring_offer_cd,
        product_key,
        dv_product_key,
        sales_order_key,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_recur_offer_keys_def_trx') }}
),

final AS (
    SELECT
        dv_recurring_offer_cd,
        product_key,
        dv_product_key,
        sales_order_key,
        sk_offer_attribution_id_int
    FROM source_wi_recur_offer_keys_def_trx
)

SELECT * FROM final
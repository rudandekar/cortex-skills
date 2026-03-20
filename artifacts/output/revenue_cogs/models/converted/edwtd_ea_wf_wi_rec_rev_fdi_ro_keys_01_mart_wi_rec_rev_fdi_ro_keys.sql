{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rec_rev_fdi_ro_keys', 'batch', 'edwtd_ea'],
    meta={
        'source_workflow': 'wf_m_WI_REC_REV_FDI_RO_KEYS',
        'target_table': 'WI_REC_REV_FDI_RO_KEYS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.088121+00:00'
    }
) }}

WITH 

source_wi_rec_rev_fdi_ro_keys AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_rec_rev_fdi_ro_keys') }}
),

final AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM source_wi_rec_rev_fdi_ro_keys
)

SELECT * FROM final
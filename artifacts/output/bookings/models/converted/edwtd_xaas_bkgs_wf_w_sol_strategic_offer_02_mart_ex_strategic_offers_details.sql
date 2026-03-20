{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sol_strategic_offer', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_W_SOL_STRATEGIC_OFFER',
        'target_table': 'EX_STRATEGIC_OFFERS_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.387897+00:00'
    }
) }}

WITH 

source_w_sol_strategic_offer AS (
    SELECT
        sales_order_line_key,
        strategic_offer_name,
        discount_percent,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sol_strategic_offer') }}
),

source_ex_strategic_offers_details AS (
    SELECT
        line_id,
        header_id,
        strategic_offer_name,
        strategic_offer_discount_pct,
        strategic_offer_version,
        edw_create_dtm,
        exception_type
    FROM {{ source('raw', 'ex_strategic_offers_details') }}
),

final AS (
    SELECT
        line_id,
        header_id,
        strategic_offer_name,
        strategic_offer_discount_pct,
        strategic_offer_version,
        edw_create_dtm,
        exception_type
    FROM source_ex_strategic_offers_details
)

SELECT * FROM final
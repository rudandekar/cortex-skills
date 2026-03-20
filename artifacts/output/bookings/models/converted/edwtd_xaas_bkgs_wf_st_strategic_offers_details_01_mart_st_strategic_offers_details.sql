{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_strategic_offers_details', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_ST_STRATEGIC_OFFERS_DETAILS',
        'target_table': 'ST_STRATEGIC_OFFERS_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.742294+00:00'
    }
) }}

WITH 

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

source_st_strategic_offers_details AS (
    SELECT
        line_id,
        header_id,
        strategic_offer_name,
        strategic_offer_discount_pct,
        strategic_offer_version,
        edw_create_dtm
    FROM {{ source('raw', 'st_strategic_offers_details') }}
),

final AS (
    SELECT
        line_id,
        header_id,
        strategic_offer_name,
        strategic_offer_discount_pct,
        strategic_offer_version,
        edw_create_dtm
    FROM source_st_strategic_offers_details
)

SELECT * FROM final
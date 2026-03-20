{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sol_strategic_offer', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_SOL_STRATEGIC_OFFER',
        'target_table': 'N_SOL_STRATEGIC_OFFER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.566480+00:00'
    }
) }}

WITH 

source_n_sol_strategic_offer AS (
    SELECT
        sales_order_line_key,
        strategic_offer_name,
        discount_percent,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_sol_strategic_offer') }}
),

final AS (
    SELECT
        sales_order_line_key,
        strategic_offer_name,
        discount_percent,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_sol_strategic_offer
)

SELECT * FROM final
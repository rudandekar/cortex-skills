{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_cx_centralized_wc', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_CX_CENTRALIZED_WC',
        'target_table': 'MT_CX_CENTRALIZED_WC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.419628+00:00'
    }
) }}

WITH 

source_mt_cx_centralized_wc AS (
    SELECT
        fiscal_year_month_int,
        hybrid_product_family,
        central_ts_platform_wc_usd_amt,
        allocated_tac_wc_usd_amt,
        wc_as_of_fiscal_year_month_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_cx_centralized_wc') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        hybrid_product_family,
        central_ts_platform_wc_usd_amt,
        allocated_tac_wc_usd_amt,
        wc_as_of_fiscal_year_month_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_cx_centralized_wc
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_profitability_fx_norm', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_PROFITABILITY_FX_NORM',
        'target_table': 'MT_PROFITABILITY_FX_NORM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.015756+00:00'
    }
) }}

WITH 

source_mt_profitability_fx_norm AS (
    SELECT
        sales_territory_key,
        bk_currency_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_profitability_fx_norm') }}
),

final AS (
    SELECT
        sales_territory_key,
        bk_currency_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_profitability_fx_norm
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cst_adj_oh_and_wrnty_pct', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CST_ADJ_OH_AND_WRNTY_PCT',
        'target_table': 'WI_CST_ADJ_OH_AND_WRNTY_PCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.997031+00:00'
    }
) }}

WITH 

source_wi_cst_adj_oh_and_wrnty_pct AS (
    SELECT
        fiscal_quarter_name,
        net_ts_fetch_coa_amt,
        total_ssc_cost,
        ssc_warranty_credit_amt,
        tac_warranty_credit_amt,
        adj_oh_cost,
        ssc_cogs_pct,
        tac_cogs_pct,
        adjusted_oh_pct
    FROM {{ source('raw', 'wi_cst_adj_oh_and_wrnty_pct') }}
),

final AS (
    SELECT
        fiscal_quarter_name,
        net_ts_fetch_coa_amt,
        total_ssc_cost,
        ssc_warranty_credit_amt,
        tac_warranty_credit_amt,
        adj_oh_cost,
        ssc_cogs_pct,
        tac_cogs_pct,
        adjusted_oh_pct
    FROM source_wi_cst_adj_oh_and_wrnty_pct
)

SELECT * FROM final
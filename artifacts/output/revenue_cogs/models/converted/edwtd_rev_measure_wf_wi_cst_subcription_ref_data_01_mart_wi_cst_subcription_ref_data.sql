{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cst_subcription_ref_data', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CST_SUBCRIPTION_REF_DATA',
        'target_table': 'WI_CST_SUBCRIPTION_REF_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.218603+00:00'
    }
) }}

WITH 

source_wi_cst_subcription_ref_data AS (
    SELECT
        fiscal_quarter_name,
        ts_fetch_amt,
        ts_coa_amt,
        net_ts_fetch_coa_amt,
        ssc_warranty_credit_amt,
        tac_warranty_credit_amt
    FROM {{ source('raw', 'wi_cst_subcription_ref_data') }}
),

final AS (
    SELECT
        fiscal_quarter_name,
        ts_fetch_amt,
        ts_coa_amt,
        net_ts_fetch_coa_amt,
        ssc_warranty_credit_amt,
        tac_warranty_credit_amt
    FROM source_wi_cst_subcription_ref_data
)

SELECT * FROM final
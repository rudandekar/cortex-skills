{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ssc_tac_wc_pf_alloc_mapping', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_SSC_TAC_WC_PF_ALLOC_MAPPING',
        'target_table': 'EL_SSC_TAC_WC_PF_ALLOC_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.667351+00:00'
    }
) }}

WITH 

source_wi_ssc_tac_wc_pf_alloc_mapping AS (
    SELECT
        fiscal_year_month_int,
        product_family,
        tac_warr_credit_amt,
        tac_alloc_warr_credit_amt,
        ssc_warr_credit_amt,
        valid_flag,
        cntr_ts_pltfom_warr_credit_amt,
        batch_id
    FROM {{ source('raw', 'wi_ssc_tac_wc_pf_alloc_mapping') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        fiscal_month_id,
        product_family,
        tac_warr_credit_amt,
        tac_alloc_warr_credit_amt,
        ssc_warr_credit_amt,
        cntr_ts_pltfom_warr_credit_amt,
        valid_flag
    FROM source_wi_ssc_tac_wc_pf_alloc_mapping
)

SELECT * FROM final
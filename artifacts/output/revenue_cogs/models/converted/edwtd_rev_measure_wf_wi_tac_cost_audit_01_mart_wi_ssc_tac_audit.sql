{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_tac_cost_audit', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_TAC_COST_AUDIT',
        'target_table': 'WI_SSC_TAC_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.762820+00:00'
    }
) }}

WITH 

source_wi_ssc_tac_audit AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        dv_source_name,
        audit_table,
        audit_date,
        dollar_mismatch_amt,
        count_mismatch
    FROM {{ source('raw', 'wi_ssc_tac_audit') }}
),

transformed_exp_ssc_cost AS (
    SELECT
    dv_fiscal_year_mth_number_int,
    dv_source_name,
    audit_table,
    audit_date,
    dollar_mismatch_amt,
    count_mismatch,
    IFF(DOLLAR_MISMATCH_AMT>1000,ABORT('$Mismatch in N_SR_TECHNI_ASSIST_CENTER_COST'),DOLLAR_MISMATCH_AMT) AS dollar_check,
    IFF(COUNT_MISMATCH<>0,ABORT('Count Mismatch in N_SR_TECHNI_ASSIST_CENTER_COST'),COUNT_MISMATCH) AS count_check
    FROM source_wi_ssc_tac_audit
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        dv_source_name,
        audit_table,
        audit_date,
        dollar_mismatch_amt,
        count_mismatch
    FROM transformed_exp_ssc_cost
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_srp_audit', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SRP_AUDIT',
        'target_table': 'WI_SRP_PNL_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.375405+00:00'
    }
) }}

WITH 

source_wi_srp_pnl_audit AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        dv_source_name,
        audit_table,
        audit_date,
        dollar_mismatch_amt
    FROM {{ source('raw', 'wi_srp_pnl_audit') }}
),

transformed_exp_srp_pnl_audit AS (
    SELECT
    dv_fiscal_year_mth_number_int,
    dv_source_name,
    audit_table,
    audit_date,
    dollar_mismatch_amt,
    IFF( DOLLAR_MISMATCH_AMT >1000,ABORT(' $ diff >1000 in MT_TSS_SERVICE_REVENUE'),DOLLAR_MISMATCH_AMT) AS dol_mismatch_check
    FROM source_wi_srp_pnl_audit
),

transformed_exp_so_agg_audit AS (
    SELECT
    dv_fiscal_year_mth_number_int,
    dv_source_name,
    audit_table,
    audit_date,
    dollar_mismatch_amt,
    IFF(DOLLAR_MISMATCH_AMT>1000,ABORT(' $ diff >1000 in MT_TSS_SERVICE_REVENUE_SO_AGG'),DOLLAR_MISMATCH_AMT) AS dollar_mismatch_check
    FROM transformed_exp_srp_pnl_audit
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        dv_source_name,
        audit_table,
        audit_date,
        dollar_mismatch_amt
    FROM transformed_exp_so_agg_audit
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_as_unbilled_revenue', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_AUDIT_AS_UNBILLED_REVENUE',
        'target_table': 'AUDIT_AS_UNBILLED_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.875621+00:00'
    }
) }}

WITH 

source_audit_as_unbilled_revenue AS (
    SELECT
        processed_fiscal_mth,
        as_ub_amt_mismatch,
        audit_trxn_type,
        audit_table,
        audit_datetimetime
    FROM {{ source('raw', 'audit_as_unbilled_revenue') }}
),

transformed_exp_audit_as_unbilled_revenue AS (
    SELECT
    processed_fiscal_mth,
    as_ub_amt_mismatch,
    audit_trxn_type,
    audit_table,
    audit_datetimetime,
    IFF(AS_UB_AMT_MISMATCH>1000, ABORT('AS_UB_AMT_MISMATCH >1000 in MT_AS_UNBILLED_REVENUE for ERP'),AS_UB_AMT_MISMATCH) AS rev_audit_check
    FROM source_audit_as_unbilled_revenue
),

transformed_exp_audit_as_unbilled_revenue2 AS (
    SELECT
    processed_fiscal_mth,
    as_ub_amt_mismatch,
    audit_trxn_type,
    audit_table,
    audit_datetimetime,
    IFF(AS_UB_AMT_MISMATCH>1000, ABORT('AS_UB_AMT_MISMATCH >1000 in MT_AS_UNBILLED_REVENUE for SBP'),AS_UB_AMT_MISMATCH) AS rev_audit_check
    FROM transformed_exp_audit_as_unbilled_revenue
),

final AS (
    SELECT
        processed_fiscal_mth,
        as_ub_amt_mismatch,
        audit_trxn_type,
        audit_table,
        audit_datetimetime
    FROM transformed_exp_audit_as_unbilled_revenue2
)

SELECT * FROM final
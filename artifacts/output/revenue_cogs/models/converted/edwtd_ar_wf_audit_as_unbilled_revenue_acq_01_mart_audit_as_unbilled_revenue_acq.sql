{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_as_unbilled_revenue_acq', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_AUDIT_AS_UNBILLED_REVENUE_ACQ',
        'target_table': 'AUDIT_AS_UNBILLED_REVENUE_ACQ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.413887+00:00'
    }
) }}

WITH 

source_audit_as_unbilled_revenue_acq AS (
    SELECT
        processed_fiscal_mth,
        as_ub_amt_mismatch,
        audit_trxn_type,
        audit_table,
        audit_datetime
    FROM {{ source('raw', 'audit_as_unbilled_revenue_acq') }}
),

transformed_exp_audit_as_unbilled_revenue1 AS (
    SELECT
    processed_fiscal_mth,
    as_ub_amt_mismatch,
    audit_trxn_type,
    audit_table,
    audit_datetime,
    IFF(AS_UB_AMT_MISMATCH>1000, ABORT('AS_UB_AMT_MISMATCH >1000 in MT_AS_UNBILLED_REVENUE for ACQ'),AS_UB_AMT_MISMATCH) AS rev_audit_check
    FROM source_audit_as_unbilled_revenue_acq
),

transformed_exp_audit_as_unbilled_revenue2 AS (
    SELECT
    processed_fiscal_mth,
    as_ub_amt_mismatch,
    audit_trxn_type,
    audit_table,
    audit_datetime,
    IFF(AS_UB_AMT_MISMATCH>1000, ABORT('AS_UB_AMT_MISMATCH >1000 in MT_AS_UNBILLED_REVENUE for ADJ'),AS_UB_AMT_MISMATCH) AS rev_audit_check
    FROM transformed_exp_audit_as_unbilled_revenue1
),

final AS (
    SELECT
        processed_fiscal_mth,
        as_ub_amt_mismatch,
        audit_trxn_type,
        audit_table,
        audit_datetime
    FROM transformed_exp_audit_as_unbilled_revenue2
)

SELECT * FROM final
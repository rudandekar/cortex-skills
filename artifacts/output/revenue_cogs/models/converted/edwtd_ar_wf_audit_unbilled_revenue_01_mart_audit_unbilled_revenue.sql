{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_unbilled_revenue', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_AUDIT_UNBILLED_REVENUE',
        'target_table': 'AUDIT_UNBILLED_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.078644+00:00'
    }
) }}

WITH 

source_audit_unbilled_revenue AS (
    SELECT
        fiscal_year_mth_int,
        rev_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM {{ source('raw', 'audit_unbilled_revenue') }}
),

transformed_exptrans AS (
    SELECT
    fiscal_year_mth_int,
    rev_mismatch_amt,
    audit_trxn_type,
    audit_table,
    audit_date,
    IFF(REV_MISMATCH_AMT>1000,ABORT('REV_MISMATCH_AMT >1000 in MT_UNBILLED_REVENUE for ERP'),REV_MISMATCH_AMT) AS rev_audit_chk
    FROM source_audit_unbilled_revenue
),

final AS (
    SELECT
        fiscal_year_mth_int,
        rev_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM transformed_exptrans
)

SELECT * FROM final
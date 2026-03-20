{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_smr_revenue', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_AUDIT_SMR_REVENUE',
        'target_table': 'AUDIT_SMR_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.899274+00:00'
    }
) }}

WITH 

source_audit_smr_revenue AS (
    SELECT
        fiscal_year_mth_int,
        rev_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM {{ source('raw', 'audit_smr_revenue') }}
),

transformed_exptrans AS (
    SELECT
    fiscal_year_mth_int,
    rev_mismatch_amt,
    audit_trxn_type,
    audit_table,
    audit_date,
    CASE WHEN REV_MISMATCH_AMT>10000,ABORT('REV_MISMATCH_AMT >10000 in MT_INV_REV_MEASURE'),REV_MISMATCH_AMT<-1000,ABORT('REV_MISMATCH_AMT <-1000 in MT_INV_REV_MEASURE'),REV_MISMATCH_AMT) AS rev_audit_check
    FROM source_audit_smr_revenue
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
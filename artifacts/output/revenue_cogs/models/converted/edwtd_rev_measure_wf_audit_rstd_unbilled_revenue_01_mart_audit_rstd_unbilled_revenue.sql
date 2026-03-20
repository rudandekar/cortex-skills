{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_rstd_unbilled_revenue', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_AUDIT_RSTD_UNBILLED_REVENUE',
        'target_table': 'AUDIT_RSTD_UNBILLED_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.770395+00:00'
    }
) }}

WITH 

source_audit_rstd_unbilled_revenue AS (
    SELECT
        processed_fiscal_mth,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM {{ source('raw', 'audit_rstd_unbilled_revenue') }}
),

transformed_ex_audit_rstd_unbilled_revenue AS (
    SELECT
    processed_fiscal_mth,
    rpo_mismatch_amt,
    audit_trxn_type,
    audit_table,
    audit_date,
    IFF(RPO_MISMATCH_AMT>1000,ABORT('RPO_MISMATCH_AMT >1000 in MT_UNBILLED_DEFERRED_REV_RPO'),RPO_MISMATCH_AMT) AS rev_audit_chk
    FROM source_audit_rstd_unbilled_revenue
),

final AS (
    SELECT
        processed_fiscal_mth,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM transformed_ex_audit_rstd_unbilled_revenue
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_tss_ub_defer_rev_rpo', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_AUDIT_TSS_UB_DEFER_REV_RPO',
        'target_table': 'AUDIT_TSS_UB_DEFER_REV_RPO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.493733+00:00'
    }
) }}

WITH 

source_audit_tss_ub_defer_rev_rpo AS (
    SELECT
        processed_fiscal_mth,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM {{ source('raw', 'audit_tss_ub_defer_rev_rpo') }}
),

transformed_exptrans AS (
    SELECT
    processed_fiscal_mth,
    rpo_mismatch_amt,
    audit_trxn_type,
    audit_table,
    audit_date,
    IFF(RPO_MISMATCH_AMT>1000,ABORT('RPO_MISMATCH_AMT >1000 IN MT_TSS_UB_DEFERRED_REV_RPO'),RPO_MISMATCH_AMT) AS rev_audit_chk
    FROM source_audit_tss_ub_defer_rev_rpo
),

final AS (
    SELECT
        processed_fiscal_mth,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM transformed_exptrans
)

SELECT * FROM final
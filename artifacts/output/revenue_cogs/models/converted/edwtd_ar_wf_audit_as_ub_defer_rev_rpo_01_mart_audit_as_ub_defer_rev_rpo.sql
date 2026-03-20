{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_as_ub_defer_rev_rpo', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_AUDIT_AS_UB_DEFER_REV_RPO',
        'target_table': 'AUDIT_AS_UB_DEFER_REV_RPO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.641709+00:00'
    }
) }}

WITH 

source_audit_as_ub_defer_rev_rpo AS (
    SELECT
        processed_fiscal_mth,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_datetime
    FROM {{ source('raw', 'audit_as_ub_defer_rev_rpo') }}
),

transformed_exp_audit_as_ub_defer_rev_rpo AS (
    SELECT
    processed_fiscal_mth,
    rpo_mismatch_amt,
    audit_trxn_type,
    audit_table,
    audit_datetime,
    IFF(RPO_MISMATCH_AMT>1000, ABORT('RPO_MISMATCH_AMT >1000 in MT_AS_UB_DEFERRED_REV_RPO'),RPO_MISMATCH_AMT) AS rev_audit_check
    FROM source_audit_as_ub_defer_rev_rpo
),

final AS (
    SELECT
        processed_fiscal_mth,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_datetime
    FROM transformed_exp_audit_as_ub_defer_rev_rpo
)

SELECT * FROM final
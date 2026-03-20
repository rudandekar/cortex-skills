{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_unbilled_defer_rev_rpo', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_AUDIT_UNBILLED_DEFER_REV_RPO',
        'target_table': 'AUDIT_UNBILLED_DEFER_REV_RPO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.637343+00:00'
    }
) }}

WITH 

source_audit_unbilled_defer_rev_rpo AS (
    SELECT
        processed_fiscal_mth,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM {{ source('raw', 'audit_unbilled_defer_rev_rpo') }}
),

transformed_exp_trans_audit_def AS (
    SELECT
    processed_fiscal_mth,
    rpo_mismatch_amt,
    audit_trxn_type,
    audit_table,
    audit_date,
    IFF(RPO_MISMATCH_AMT>1000,ABORT('RPO_MISMATCH_AMT >1000 in MT_UNBILLED_DEFERRED_REV_RPO'),RPO_MISMATCH_AMT) AS rev_audit_chk
    FROM source_audit_unbilled_defer_rev_rpo
),

final AS (
    SELECT
        processed_fiscal_mth,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM transformed_exp_trans_audit_def
)

SELECT * FROM final
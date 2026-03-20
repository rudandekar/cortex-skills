{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_tss_unbilled_revenue', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_AUDIT_TSS_UNBILLED_REVENUE',
        'target_table': 'AUDIT_TSS_UNBILLED_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.350759+00:00'
    }
) }}

WITH 

source_audit_tss_unbilled_revenue AS (
    SELECT
        processed_fiscal_mth,
        tss_ub_amt_mismatch,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM {{ source('raw', 'audit_tss_unbilled_revenue') }}
),

transformed_exptrans1 AS (
    SELECT
    processed_fiscal_mth,
    tss_ub_amt_mismatch,
    audit_trxn_type,
    audit_table,
    audit_date,
    IFF(TSS_UB_AMT_MISMATCH>1000,ABORT('TSS_UB_AMT_MISMATCH >1000 IN MT_TSS_UNBILLED_REVENUE FOR ERP'),TSS_UB_AMT_MISMATCH) AS rev_audit_chk
    FROM source_audit_tss_unbilled_revenue
),

transformed_exptrans2 AS (
    SELECT
    processed_fiscal_mth,
    tss_ub_amt_mismatch,
    audit_trxn_type,
    audit_table,
    audit_date,
    IFF(TSS_UB_AMT_MISMATCH>1000,ABORT('TSS_UB_AMT_MISMATCH >1000 IN MT_TSS_UNBILLED_REVENUE FOR ERP'),TSS_UB_AMT_MISMATCH) AS rev_audit_chk
    FROM transformed_exptrans1
),

final AS (
    SELECT
        processed_fiscal_mth,
        tss_ub_amt_mismatch,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM transformed_exptrans2
)

SELECT * FROM final
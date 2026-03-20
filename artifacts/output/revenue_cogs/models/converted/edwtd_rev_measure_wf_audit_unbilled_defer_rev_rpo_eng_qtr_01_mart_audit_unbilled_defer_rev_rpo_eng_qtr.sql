{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_unbilled_defer_rev_rpo_eng_qtr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_AUDIT_UNBILLED_DEFER_REV_RPO_ENG_QTR',
        'target_table': 'AUDIT_UNBILLED_DEFER_REV_RPO_ENG_QTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.073676+00:00'
    }
) }}

WITH 

source_audit_unbilled_defer_rev_rpo_eng_qtr AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM {{ source('raw', 'audit_unbilled_defer_rev_rpo_eng_qtr') }}
),

transformed_exp_audit_unbilled_defer_rev_rpo_qtr AS (
    SELECT
    fiscal_year_quarter_number_int,
    fiscal_year_month_int,
    rpo_mismatch_amt,
    audit_trxn_type,
    audit_table,
    audit_date,
    IFF(RPO_MISMATCH_AMT>1000,ABORT('RPO_MISMATCH_AMT >1000 in MT_UNBILLED_REVENUE_RPO_QTR for ERP'),RPO_MISMATCH_AMT) AS rev_audit_chk
    FROM source_audit_unbilled_defer_rev_rpo_eng_qtr
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_date
    FROM transformed_exp_audit_unbilled_defer_rev_rpo_qtr
)

SELECT * FROM final
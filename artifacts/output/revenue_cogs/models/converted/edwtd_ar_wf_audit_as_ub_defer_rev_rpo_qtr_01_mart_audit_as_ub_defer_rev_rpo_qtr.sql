{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_as_ub_defer_rev_rpo_qtr', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_AUDIT_AS_UB_DEFER_REV_RPO_QTR',
        'target_table': 'AUDIT_AS_UB_DEFER_REV_RPO_QTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.651989+00:00'
    }
) }}

WITH 

source_audit_as_ub_defer_rev_rpo_qtr AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_datetime
    FROM {{ source('raw', 'audit_as_ub_defer_rev_rpo_qtr') }}
),

transformed_exp_audit_as_ub_defer_rev_rpo_qtr AS (
    SELECT
    fiscal_year_quarter_number_int,
    fiscal_year_month_int,
    rpo_mismatch_amt,
    audit_trxn_type,
    audit_table,
    audit_datetime,
    IFF(RPO_MISMATCH_AMT>1000, ABORT('RPO_MISMATCH_AMT >1000 in MT_AS_UB_DEFER_REV_RPO_QTR'),RPO_MISMATCH_AMT) AS rev_audit_check
    FROM source_audit_as_ub_defer_rev_rpo_qtr
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        rpo_mismatch_amt,
        audit_trxn_type,
        audit_table,
        audit_datetime
    FROM transformed_exp_audit_as_ub_defer_rev_rpo_qtr
)

SELECT * FROM final
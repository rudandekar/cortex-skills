{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_as_rev_audit', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_AS_REV_AUDIT',
        'target_table': 'WI_PNL_AS_REV_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.356269+00:00'
    }
) }}

WITH 

source_wi_pnl_as_rev_audit AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        dv_source_name,
        audit_table,
        audit_date,
        dollar_mismatch_amt
    FROM {{ source('raw', 'wi_pnl_as_rev_audit') }}
),

transformed_exp_pnl_adj AS (
    SELECT
    dv_fiscal_year_mth_number_int,
    dv_source_name,
    audit_table,
    audit_date,
    dollar_mismatch_amt,
    IFF(DOLLAR_MISMATCH_AMT>1000,ABORT(' $ diff >1000 in MT_PNL_REV_COST_MSR'),DOLLAR_MISMATCH_AMT) AS dollar_mismatch_check
    FROM source_wi_pnl_as_rev_audit
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        dv_source_name,
        audit_table,
        audit_date,
        dollar_mismatch_amt
    FROM transformed_exp_pnl_adj
)

SELECT * FROM final
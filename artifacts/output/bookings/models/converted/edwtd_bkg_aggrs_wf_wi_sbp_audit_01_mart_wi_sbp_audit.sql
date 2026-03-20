{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sbp_audit', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SBP_AUDIT',
        'target_table': 'WI_SBP_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.519758+00:00'
    }
) }}

WITH 

source_wi_sbp_audit AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        audit_table,
        audit_date,
        dollar_mismatch_amt
    FROM {{ source('raw', 'wi_sbp_audit') }}
),

transformed_exp_bkgs_mismatch AS (
    SELECT
    dv_fiscal_year_mth_number_int,
    audit_table,
    audit_date,
    dollar_mismatch_amt,
    IIF (DOLLAR_MISMATCH_AMT>ABS(1000) ,ABORT('Mismatch>$1000 in MT_SERVICES_BOOKINGS'),DOLLAR_MISMATCH_AMT) AS mismatch_status
    FROM source_wi_sbp_audit
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        audit_table,
        audit_date,
        dollar_mismatch_amt
    FROM transformed_exp_bkgs_mismatch
)

SELECT * FROM final
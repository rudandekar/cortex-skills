{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfi_rae_adjstmt_scd', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFI_RAE_ADJSTMT_SCD',
        'target_table': 'EL_XXCFI_RAE_ADJSTMT_SCD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.121279+00:00'
    }
) }}

WITH 

source_el_xxcfi_rae_adjstmt_scd AS (
    SELECT
        schedule_id,
        gl_date,
        schedule_line_amt,
        schedule_line_acctd_amt,
        adjustment_currency_code,
        debit_segment4,
        credit_segment4,
        debit_ccid,
        credit_ccid,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code
    FROM {{ source('raw', 'el_xxcfi_rae_adjstmt_scd') }}
),

final AS (
    SELECT
        schedule_id,
        gl_date,
        schedule_line_amt,
        schedule_line_acctd_amt,
        adjustment_currency_code,
        debit_segment4,
        credit_segment4,
        debit_ccid,
        credit_ccid,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code
    FROM source_el_xxcfi_rae_adjstmt_scd
)

SELECT * FROM final
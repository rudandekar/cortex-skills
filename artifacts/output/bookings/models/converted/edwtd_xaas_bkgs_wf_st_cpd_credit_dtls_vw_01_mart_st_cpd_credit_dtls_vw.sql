{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cpd_credit_dtls_vw', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_ST_CPD_CREDIT_DTLS_VW',
        'target_table': 'ST_CPD_CREDIT_DTLS_VW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.815645+00:00'
    }
) }}

WITH 

source_ff_cpd_credit_dtls_vw AS (
    SELECT
        credit_code,
        credit_name,
        credit_category,
        effective_start_date,
        effective_end_date,
        last_update_date
    FROM {{ source('raw', 'ff_cpd_credit_dtls_vw') }}
),

final AS (
    SELECT
        credit_code,
        credit_name,
        credit_category,
        effective_start_date,
        effective_end_date,
        last_update_date
    FROM source_ff_cpd_credit_dtls_vw
)

SELECT * FROM final
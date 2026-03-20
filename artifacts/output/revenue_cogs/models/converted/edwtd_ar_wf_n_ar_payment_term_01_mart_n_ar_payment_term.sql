{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_payment_term', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_PAYMENT_TERM',
        'target_table': 'N_AR_PAYMENT_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.558426+00:00'
    }
) }}

WITH 

source_w_ar_payment_term AS (
    SELECT
        bk_ar_payment_term_name,
        ar_payment_term_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_payment_term') }}
),

final AS (
    SELECT
        bk_ar_payment_term_name,
        ar_payment_term_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ar_payment_term
)

SELECT * FROM final
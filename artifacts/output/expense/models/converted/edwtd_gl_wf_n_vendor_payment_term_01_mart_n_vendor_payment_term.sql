{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_vendor_payment_term', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_VENDOR_PAYMENT_TERM',
        'target_table': 'N_VENDOR_PAYMENT_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.117317+00:00'
    }
) }}

WITH 

source_w_vendor_payment_term AS (
    SELECT
        vendor_payment_term_key,
        sk_term_id_int,
        payment_term_name,
        payment_term_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_vendor_payment_term') }}
),

final AS (
    SELECT
        vendor_payment_term_key,
        sk_term_id_int,
        payment_term_name,
        payment_term_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_vendor_payment_term
)

SELECT * FROM final
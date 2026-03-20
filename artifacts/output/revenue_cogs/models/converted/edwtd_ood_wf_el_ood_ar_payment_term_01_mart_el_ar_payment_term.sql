{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ood_ar_payment_term', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_EL_OOD_AR_PAYMENT_TERM',
        'target_table': 'EL_AR_PAYMENT_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.127614+00:00'
    }
) }}

WITH 

source_st_om_ar_pymnt_term AS (
    SELECT
        description,
        global_name,
        om_language,
        name,
        term_id,
        creation_date,
        last_update_date,
        ges_update_date,
        create_datetime,
        batch_id,
        action_code
    FROM {{ source('raw', 'st_om_ar_pymnt_term') }}
),

final AS (
    SELECT
        bk_ar_payment_term_name,
        ar_payment_term_descr,
        global_name,
        sk_payment_term_id,
        edw_create_dtm,
        edw_update_dtm,
        identifier
    FROM source_st_om_ar_pymnt_term
)

SELECT * FROM final
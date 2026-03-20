{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ar_payment_term', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_AR_PAYMENT_TERM',
        'target_table': 'W_AR_PAYMENT_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.330954+00:00'
    }
) }}

WITH 

source_st_cg_ar_pymnt_term_rev AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        description,
        cg_language,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        source_lang,
        term_id,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg_ar_pymnt_term_rev') }}
),

final AS (
    SELECT
        bk_ar_payment_term_name,
        ar_payment_term_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_cg_ar_pymnt_term_rev
)

SELECT * FROM final
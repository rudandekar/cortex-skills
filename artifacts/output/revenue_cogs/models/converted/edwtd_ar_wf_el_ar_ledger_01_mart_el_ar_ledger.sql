{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ar_ledger', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_AR_LEDGER',
        'target_table': 'EL_AR_LEDGER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.072819+00:00'
    }
) }}

WITH 

source_n_set_of_books AS (
    SELECT
        set_of_books_key,
        allow_intercompany_post_flag,
        bk_chart_of_accounts_id_int,
        bk_set_of_books_name,
        functional_currency_code,
        set_of_books_description,
        set_of_books_short_name,
        sk_set_of_books_id_int,
        ss_code,
        edw_update_user,
        edw_create_user,
        edw_create_datetime,
        edw_update_datetime,
        multireporting_currncy_type_cd,
        push_to_glbl_consol_books_flg
    FROM {{ source('raw', 'n_set_of_books') }}
),

final AS (
    SELECT
        set_of_books_key,
        bk_set_of_books_name,
        functional_currency_code,
        ledger_id,
        push_to_glbl_consol_books_flg,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM source_n_set_of_books
)

SELECT * FROM final
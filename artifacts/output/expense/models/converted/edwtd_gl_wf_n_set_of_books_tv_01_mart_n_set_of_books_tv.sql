{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_set_of_books_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_SET_OF_BOOKS_TV',
        'target_table': 'N_SET_OF_BOOKS_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.842413+00:00'
    }
) }}

WITH 

source_w_set_of_books AS (
    SELECT
        start_tv_dt,
        set_of_books_key,
        end_tv_dt,
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
        action_code,
        dml_type,
        multireporting_currncy_type_cd
    FROM {{ source('raw', 'w_set_of_books') }}
),

source_n_set_of_books_tv AS (
    SELECT
        start_tv_dt,
        set_of_books_key,
        end_tv_dt,
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
        push_to_glbl_consol_books_flg,
        parent_set_of_books_key
    FROM {{ source('raw', 'n_set_of_books_tv') }}
),

final AS (
    SELECT
        start_tv_dt,
        set_of_books_key,
        end_tv_dt,
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
        push_to_glbl_consol_books_flg,
        parent_set_of_books_key
    FROM source_n_set_of_books_tv
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_trx_type_tv', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_TRX_TYPE_TV',
        'target_table': 'N_AR_TRX_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.510652+00:00'
    }
) }}

WITH 

source_n_ar_trx_type_tv AS (
    SELECT
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        start_tv_date,
        end_tv_date,
        ar_trx_type_short_code,
        ar_trx_type_description,
        sk_cust_trx_type_id_int,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_trx_type_tv') }}
),

source_w_ar_trx_type AS (
    SELECT
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        start_tv_date,
        end_tv_date,
        ar_trx_type_short_code,
        ar_trx_type_description,
        sk_cust_trx_type_id_int,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_trx_type') }}
),

final AS (
    SELECT
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        ar_trx_type_short_code,
        ar_trx_type_description,
        sk_cust_trx_type_id_int,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_w_ar_trx_type
)

SELECT * FROM final
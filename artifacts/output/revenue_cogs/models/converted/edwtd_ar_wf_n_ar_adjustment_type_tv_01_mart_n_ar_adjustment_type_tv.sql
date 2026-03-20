{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_adjustment_type_tv', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_ADJUSTMENT_TYPE_TV',
        'target_table': 'N_AR_ADJUSTMENT_TYPE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.507390+00:00'
    }
) }}

WITH 

source_n_ar_adjustment_type_tv AS (
    SELECT
        bk_ar_adj_type_code,
        bk_set_of_books_key,
        bk_company_code,
        start_tv_date,
        end_tv_date,
        adjustment_type_description,
        adjustment_type_name,
        cr_general_ledger_account_key,
        dr_general_ledger_account_key,
        sk_adjustment_type_id_int,
        ss_source_application_code,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_adjustment_type_tv') }}
),

source_w_ar_adjustment_type AS (
    SELECT
        bk_ar_adj_type_code,
        bk_set_of_books_key,
        bk_company_code,
        start_tv_date,
        end_tv_date,
        adjustment_type_description,
        adjustment_type_name,
        cr_general_ledger_account_key,
        dr_general_ledger_account_key,
        sk_adjustment_type_id_int,
        ss_source_application_code,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_adjustment_type') }}
),

final AS (
    SELECT
        bk_ar_adj_type_code,
        bk_set_of_books_key,
        bk_company_code,
        start_tv_date,
        end_tv_date,
        adjustment_type_description,
        adjustment_type_name,
        cr_general_ledger_account_key,
        dr_general_ledger_account_key,
        sk_adjustment_type_id_int,
        ss_source_application_code,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_w_ar_adjustment_type
)

SELECT * FROM final
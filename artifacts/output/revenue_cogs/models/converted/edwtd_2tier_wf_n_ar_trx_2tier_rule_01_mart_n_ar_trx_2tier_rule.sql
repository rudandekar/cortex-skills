{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_trx_2tier_rule', 'batch', 'edwtd_2tier'],
    meta={
        'source_workflow': 'wf_m_N_AR_TRX_2TIER_RULE',
        'target_table': 'N_AR_TRX_2TIER_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.429902+00:00'
    }
) }}

WITH 

source_w_ar_trx_2tier_rule AS (
    SELECT
        bk_financial_account_cd,
        bk_fiscal_calendar_cd,
        bk_start_fiscal_year_num_int,
        bk_start_fiscal_month_num_int,
        bk_cfi_theater_name,
        distribution_type_cd,
        two_tier_rule_name,
        reserve_percent_rt,
        set_of_books_key,
        operating_unit_name_cd,
        end_fiscal_year_num_int,
        end_fiscal_month_num_int,
        sk_rule_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_trx_2tier_rule') }}
),

final AS (
    SELECT
        bk_financial_account_cd,
        bk_fiscal_calendar_cd,
        bk_start_fiscal_year_num_int,
        bk_start_fiscal_month_num_int,
        bk_cfi_theater_name,
        distribution_type_cd,
        two_tier_rule_name,
        reserve_percent_rt,
        set_of_books_key,
        operating_unit_name_cd,
        end_fiscal_year_num_int,
        end_fiscal_month_num_int,
        sk_rule_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ar_trx_2tier_rule
)

SELECT * FROM final
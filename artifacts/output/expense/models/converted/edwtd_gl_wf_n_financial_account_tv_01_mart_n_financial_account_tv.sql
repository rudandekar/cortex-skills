{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_account_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_ACCOUNT_TV',
        'target_table': 'N_FINANCIAL_ACCOUNT_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.946654+00:00'
    }
) }}

WITH 

source_w_financial_account AS (
    SELECT
        bk_financial_account_code,
        ru_bk_parent_fin_acct_code,
        account_group_id,
        start_tv_date,
        end_tv_date,
        financial_account_type,
        financial_account_description,
        financial_acct_start_date,
        global_account_owner_party_key,
        edw_create_datetime,
        edw_update_datetime,
        parent_role,
        edw_update_user,
        edw_create_user,
        financial_acct_display_seq_int,
        controllable_account_flg,
        gps_spend_category_cd,
        gps_spend_sub_category_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_financial_account') }}
),

source_n_financial_account_tv AS (
    SELECT
        bk_financial_account_code,
        ru_bk_parent_fin_acct_code,
        account_group_id,
        start_tv_date,
        end_tv_date,
        financial_account_type,
        financial_account_description,
        financial_acct_start_date,
        global_account_owner_party_key,
        edw_create_datetime,
        edw_update_datetime,
        parent_role,
        edw_update_user,
        edw_create_user,
        financial_acct_display_seq_int,
        controllable_account_flg,
        gps_spend_category_cd,
        gps_spend_sub_category_cd
    FROM {{ source('raw', 'n_financial_account_tv') }}
),

final AS (
    SELECT
        bk_financial_account_code,
        ru_bk_parent_fin_acct_code,
        account_group_id,
        start_tv_date,
        end_tv_date,
        financial_account_type,
        financial_account_description,
        financial_acct_start_date,
        global_account_owner_party_key,
        edw_create_datetime,
        edw_update_datetime,
        parent_role,
        edw_update_user,
        edw_create_user,
        financial_acct_display_seq_int,
        controllable_account_flg,
        exclude_it_allocations_flg,
        gps_spend_category_cd,
        gps_spend_sub_category_cd
    FROM source_n_financial_account_tv
)

SELECT * FROM final
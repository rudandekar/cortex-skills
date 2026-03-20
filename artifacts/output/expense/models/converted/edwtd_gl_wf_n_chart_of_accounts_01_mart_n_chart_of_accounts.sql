{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_chart_of_accounts', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_CHART_OF_ACCOUNTS',
        'target_table': 'N_CHART_OF_ACCOUNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.051535+00:00'
    }
) }}

WITH 

source_w_chart_of_accounts AS (
    SELECT
        bk_chart_of_accounts_id_int,
        chart_of_accounts_name,
        chart_of_accounts_description,
        bk_project_locality_int,
        bk_subaccount_locality_int,
        bk_fin_acct_locality_int,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code
    FROM {{ source('raw', 'w_chart_of_accounts') }}
),

final AS (
    SELECT
        bk_chart_of_accounts_id_int,
        chart_of_accounts_name,
        chart_of_accounts_description,
        bk_project_locality_int,
        bk_subaccount_locality_int,
        bk_fin_acct_locality_int,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_w_chart_of_accounts
)

SELECT * FROM final
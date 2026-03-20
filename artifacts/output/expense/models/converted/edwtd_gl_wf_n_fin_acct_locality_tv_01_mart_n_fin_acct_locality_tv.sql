{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_acct_locality_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIN_ACCT_LOCALITY_TV',
        'target_table': 'N_FIN_ACCT_LOCALITY_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.126490+00:00'
    }
) }}

WITH 

source_w_fin_acct_locality AS (
    SELECT
        start_tv_date,
        end_tv_date,
        financial_acct_locality_name,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_si_flex_struct_id_int,
        bk_fin_acct_locality_int,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fin_acct_locality') }}
),

final AS (
    SELECT
        start_tv_date,
        end_tv_date,
        financial_acct_locality_name,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_si_flex_struct_id_int,
        bk_fin_acct_locality_int
    FROM source_w_fin_acct_locality
)

SELECT * FROM final
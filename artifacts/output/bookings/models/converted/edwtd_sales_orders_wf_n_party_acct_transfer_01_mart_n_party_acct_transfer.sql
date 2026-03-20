{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_party_acct_transfer', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_PARTY_ACCT_TRANSFER',
        'target_table': 'N_PARTY_ACCT_TRANSFER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.675847+00:00'
    }
) }}

WITH 

source_n_party_acct_transfer AS (
    SELECT
        party_key,
        bk_fiscal_year,
        acct_transfer_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        acct_transfer_reason_descr
    FROM {{ source('raw', 'n_party_acct_transfer') }}
),

final AS (
    SELECT
        party_key,
        bk_fiscal_year,
        acct_transfer_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        acct_transfer_reason_descr
    FROM source_n_party_acct_transfer
)

SELECT * FROM final
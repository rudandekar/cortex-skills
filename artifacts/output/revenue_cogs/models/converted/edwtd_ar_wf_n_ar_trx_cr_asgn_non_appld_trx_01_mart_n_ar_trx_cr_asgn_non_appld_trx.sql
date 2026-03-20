{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_trx_cr_asgn_non_appld_trx', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_TRX_CR_ASGN_NON_APPLD_TRX',
        'target_table': 'N_AR_TRX_CR_ASGN_NON_APPLD_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.133259+00:00'
    }
) }}

WITH 

source_w_ar_trx_cr_asgn_non_appld_trx AS (
    SELECT
        ar_trx_cr_asgn_non_applied_key,
        source_deleted_flg,
        sales_commission_pct,
        source_last_update_dtm,
        source_create_dtm,
        sales_rep_num,
        sales_territory_key,
        sales_credit_type_cd,
        ar_transaction_key,
        sk_header_sequence_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'w_ar_trx_cr_asgn_non_appld_trx') }}
),

final AS (
    SELECT
        ar_trx_cr_asgn_non_applied_key,
        source_deleted_flg,
        sales_commission_pct,
        source_last_update_dtm,
        source_create_dtm,
        sales_rep_num,
        sales_territory_key,
        sales_credit_type_cd,
        ar_transaction_key,
        sk_header_sequence_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ar_trx_cr_asgn_non_appld_trx
)

SELECT * FROM final
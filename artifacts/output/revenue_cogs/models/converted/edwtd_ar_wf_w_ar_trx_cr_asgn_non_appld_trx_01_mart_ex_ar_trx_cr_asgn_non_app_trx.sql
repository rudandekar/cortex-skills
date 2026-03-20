{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ar_trx_cr_asgn_non_appld_trx', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_AR_TRX_CR_ASGN_NON_APPLD_TRX',
        'target_table': 'EX_AR_TRX_CR_ASGN_NON_APP_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.632627+00:00'
    }
) }}

WITH 

source_sm_ar_trx_cr_asgn_non_apld_trx AS (
    SELECT
        ar_trx_cr_asgn_non_applied_key,
        sk_header_sequence_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_ar_trx_cr_asgn_non_apld_trx') }}
),

source_wi_ar_trx_cr_asgn_non_app_trx AS (
    SELECT
        source_deleted_flg,
        assignment_mode,
        copy_from_minx_order_number,
        copy_from_source_header_id,
        copy_from_source_type,
        created_by,
        creation_date,
        deal_scmt_id,
        ges_update_date,
        global_name,
        header_seq_id,
        interfaced_to_cdw_flag,
        last_updated_by,
        last_update_date,
        last_update_login,
        minx_order_number,
        salesrep_id,
        sales_credit_type_id,
        source_header_id,
        source_type,
        split_percent,
        territory_id
    FROM {{ source('raw', 'wi_ar_trx_cr_asgn_non_app_trx') }}
),

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
        assignment_mode,
        copy_from_minx_order_number,
        copy_from_source_header_id,
        copy_from_source_type,
        created_by,
        creation_date,
        deal_scmt_id,
        ges_update_date,
        global_name,
        header_seq_id,
        interfaced_to_cdw_flag,
        last_updated_by,
        last_update_date,
        last_update_login,
        minx_order_number,
        salesrep_id,
        sales_credit_type_id,
        source_header_id,
        source_type,
        split_percent,
        territory_id
    FROM source_w_ar_trx_cr_asgn_non_appld_trx
)

SELECT * FROM final
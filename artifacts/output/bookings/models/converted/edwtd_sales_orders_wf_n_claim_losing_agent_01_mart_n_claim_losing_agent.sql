{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_claim_losing_agent', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CLAIM_LOSING_AGENT',
        'target_table': 'N_CLAIM_LOSING_AGENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.416126+00:00'
    }
) }}

WITH 

source_w_claim_losing_agent AS (
    SELECT
        claim_losing_agent_key,
        sk_trx_src_id,
        lst_uptd_by_csco_wrkr_prty_key,
        claim_aprvr_csco_wrkr_prty_key,
        claim_key,
        sales_order_line_key,
        crtd_by_csco_wrkr_prty_key,
        bk_claim_losing_sales_rep_num,
        claim_rejection_rsn_cd,
        sales_territory_key,
        last_update_dtm,
        split_claim_value_amt,
        trx_type_cd,
        claim_losing_version_id_int,
        claim_trx_type_cd,
        src_trx_id_int,
        trx_src_type_cd,
        create_dtm,
        claim_sales_credit_id_int,
        losing_agent_status_name,
        partner_name,
        sk_trx_split_bu_key_id,
        claim_rejection_reason_descr,
        bk_pos_transaction_id_int,
        so_sbscrptn_itm_sls_trx_key,
        manual_trx_key,
        ar_trx_line_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_claim_losing_agent') }}
),

final AS (
    SELECT
        claim_losing_agent_key,
        sk_trx_src_id,
        lst_uptd_by_csco_wrkr_prty_key,
        claim_aprvr_csco_wrkr_prty_key,
        claim_key,
        sales_order_line_key,
        crtd_by_csco_wrkr_prty_key,
        bk_claim_losing_sales_rep_num,
        claim_rejection_rsn_cd,
        sales_territory_key,
        last_update_dtm,
        split_claim_value_amt,
        trx_type_cd,
        claim_losing_version_id_int,
        claim_trx_type_cd,
        src_trx_id_int,
        trx_src_type_cd,
        create_dtm,
        claim_sales_credit_id_int,
        losing_agent_status_name,
        partner_name,
        sk_trx_split_bu_key_id,
        claim_rejection_reason_descr,
        bk_pos_transaction_id_int,
        so_sbscrptn_itm_sls_trx_key,
        manual_trx_key,
        ar_trx_line_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_claim_losing_agent
)

SELECT * FROM final
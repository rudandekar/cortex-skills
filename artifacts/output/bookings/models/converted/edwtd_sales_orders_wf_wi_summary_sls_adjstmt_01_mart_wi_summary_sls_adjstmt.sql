{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_summary_sls_adjstmt', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SUMMARY_SLS_ADJSTMT',
        'target_table': 'WI_SUMMARY_SLS_ADJSTMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.248501+00:00'
    }
) }}

WITH 

source_wi_summary_sls_adjstmt AS (
    SELECT
        summary_adj_key,
        original_sales_terr_key,
        restated_sales_terr_key,
        sales_order_line_key,
        bk_bookings_trx_type_cd,
        allocation_split_pct,
        effective_dt,
        sk_trx_id_bigint,
        restatement_sub_type_cd,
        cust_prty_key,
        do_not_restate_flg,
        expiration_dt,
        apprvd_by_csco_wrkr_prty_key,
        uploaded_by_csco_wrkr_prty_key,
        approved_dtm,
        dv_approved_dt,
        bk_uploaded_dtm,
        dv_uploaded_dt,
        validation_status_cd,
        validation_status_reason_descr,
        bk_upload_file_name,
        processed_flg,
        ar_trx_line_key,
        bk_pos_trx_id_int,
        bk_sales_adj_line_num_int,
        so_subscr_item_sales_trx_key,
        rev_transfer_key,
        sales_acct_grp_prty_key,
        dv_bookings_trx_ref_id,
        original_cust_prty_key,
        original_sls_acct_grp_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        reason_comment_txt,
        summary_adjustment_flg,
        bk_deal_id,
        sales_rep_num
    FROM {{ source('raw', 'wi_summary_sls_adjstmt') }}
),

final AS (
    SELECT
        summary_adj_key,
        original_sales_terr_key,
        restated_sales_terr_key,
        sales_order_line_key,
        bk_bookings_trx_type_cd,
        allocation_split_pct,
        effective_dt,
        sk_trx_id_bigint,
        restatement_sub_type_cd,
        cust_prty_key,
        do_not_restate_flg,
        expiration_dt,
        apprvd_by_csco_wrkr_prty_key,
        uploaded_by_csco_wrkr_prty_key,
        approved_dtm,
        dv_approved_dt,
        bk_uploaded_dtm,
        dv_uploaded_dt,
        validation_status_cd,
        validation_status_reason_descr,
        bk_upload_file_name,
        processed_flg,
        ar_trx_line_key,
        bk_pos_trx_id_int,
        bk_sales_adj_line_num_int,
        so_subscr_item_sales_trx_key,
        rev_transfer_key,
        sales_acct_grp_prty_key,
        dv_bookings_trx_ref_id,
        original_cust_prty_key,
        original_sls_acct_grp_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        reason_comment_txt,
        summary_adjustment_flg,
        bk_deal_id,
        sales_rep_num
    FROM source_wi_summary_sls_adjstmt
)

SELECT * FROM final
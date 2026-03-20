{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_summary_dyn_sls_adj_upld', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SUMMARY_DYN_SLS_ADJ_UPLD',
        'target_table': 'WI_SUMMARY_DYN_SLS_ADJ_UPLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.155134+00:00'
    }
) }}

WITH 

source_wi_summary_dyn_sls_adj_upld AS (
    SELECT
        bk_src_rptd_upld_by_cec_id,
        bk_upload_file_name,
        bk_uploaded_dtm,
        bk_upload_file_line_num_int,
        dv_uploaded_dt,
        restatement_sub_type_cd,
        src_rptd_org_sls_terr_name_cd,
        allocation_split_pct,
        src_rptd_rstd_sls_terr_name_cd,
        do_not_restate_flg,
        effective_dt,
        expiration_dt,
        src_rptd_approved_by_cec_id,
        approved_dtm,
        dv_approved_dt,
        validation_status_cd,
        validation_status_reason_descr,
        processed_flg,
        reason_comment_txt,
        bookings_trx_sub_type_cd,
        src_rptd_original_cust_prty_id,
        original_sales_acct_id,
        dyn_restmt_sls_adjstmt_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_deal_id,
        sales_rep_num,
        service_indicator_flag,
        dv_fiscal_quarter_id,
        product_id
    FROM {{ source('raw', 'wi_summary_dyn_sls_adj_upld') }}
),

final AS (
    SELECT
        bk_src_rptd_upld_by_cec_id,
        bk_upload_file_name,
        bk_uploaded_dtm,
        bk_upload_file_line_num_int,
        dv_uploaded_dt,
        restatement_sub_type_cd,
        src_rptd_org_sls_terr_name_cd,
        allocation_split_pct,
        src_rptd_rstd_sls_terr_name_cd,
        do_not_restate_flg,
        effective_dt,
        expiration_dt,
        src_rptd_approved_by_cec_id,
        approved_dtm,
        dv_approved_dt,
        validation_status_cd,
        validation_status_reason_descr,
        processed_flg,
        reason_comment_txt,
        bookings_trx_sub_type_cd,
        src_rptd_original_cust_prty_id,
        original_sales_acct_id,
        dyn_restmt_sls_adjstmt_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_deal_id,
        sales_rep_num,
        service_indicator_flag,
        dv_fiscal_quarter_id,
        product_id
    FROM source_wi_summary_dyn_sls_adj_upld
)

SELECT * FROM final
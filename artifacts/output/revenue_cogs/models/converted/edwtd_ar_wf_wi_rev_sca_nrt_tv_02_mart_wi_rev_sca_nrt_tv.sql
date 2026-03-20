{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rev_sca_nrt_tv', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_REV_SCA_NRT_TV',
        'target_table': 'WI_REV_SCA_NRT_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.351440+00:00'
    }
) }}

WITH 

source_wi_rev_sca_nrt_incr AS (
    SELECT
        sales_credit_assignment_key,
        sca_source_type_cd,
        sk_line_seq_id_int,
        ss_cd,
        ep_source_line_id_int,
        ep_sk_salesrep_id_int,
        sca_sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        sca_source_commit_dtm,
        edw_create_dtm,
        edw_update_dtm,
        ru_sales_order_line_key,
        ru_ar_transaction_line_key,
        ep_sk_sales_credit_type_id_int,
        ep_sk_territory_id_int,
        quota_flag,
        bk_sales_credit_type_code,
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        dv_fiscal_year_month_num_int,
        event_type,
        sales_rep_number,
        sales_territory_key
    FROM {{ source('raw', 'wi_rev_sca_nrt_incr') }}
),

source_n_sca_for_all_trx_nrt_hist_tv AS (
    SELECT
        sales_credit_assignment_key,
        ep_source_line_id_int,
        sca_transaction_type,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sk_line_seq_id_int,
        ss_cd,
        sca_source_type_cd,
        sca_sales_commission_pct,
        bk_sales_credit_type_code,
        ep_sk_sales_credit_type_id_int,
        sales_rep_number,
        ep_sk_salesrep_id_int,
        sales_territory_key,
        ep_sk_territory_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_ar_transaction_line_key,
        ru_sales_order_line_key,
        start_tv_dtm,
        end_tv_dtm
    FROM {{ source('raw', 'n_sca_for_all_trx_nrt_hist_tv') }}
),

final AS (
    SELECT
        sales_credit_assignment_key,
        sca_source_type_cd,
        sk_line_seq_id_int,
        ss_cd,
        ep_source_line_id_int,
        ep_sk_salesrep_id_int,
        sca_sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        sca_source_commit_dtm,
        edw_create_dtm,
        edw_update_dtm,
        ru_sales_order_line_key,
        ru_ar_transaction_line_key,
        ep_sk_sales_credit_type_id_int,
        ep_sk_territory_id_int,
        quota_flag,
        bk_sales_credit_type_code,
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        dv_fiscal_year_month_num_int,
        event_type,
        sales_rep_number,
        sales_territory_key,
        sk_sc_agent_id_int
    FROM source_n_sca_for_all_trx_nrt_hist_tv
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sca_retro_nrt', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_WI_SCA_RETRO_NRT',
        'target_table': 'WI_SCA_RETRO_NRT_2',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.138178+00:00'
    }
) }}

WITH 

source_wi_sca_retro_nrt_2 AS (
    SELECT
        sales_credit_assignment_key,
        ep_source_line_id_int,
        sca_source_commit_dtm,
        sca_source_update_dtm,
        ss_cd,
        global_name,
        sales_rep_number,
        new_sales_rep_number,
        ep_sk_salesrep_id_int,
        sales_territory_key,
        new_sales_territory_key,
        ep_sk_territory_id_int,
        ru_sales_order_line_key,
        new_ru_sales_order_line_key,
        bk_sales_credit_type_code,
        new_bk_sales_credit_type_code,
        ep_sk_sales_credit_type_id_int,
        sca_source_type_cd
    FROM {{ source('raw', 'wi_sca_retro_nrt_2') }}
),

source_wi_sca_retro_nrt_1 AS (
    SELECT
        sales_credit_assignment_key,
        ep_source_line_id_int,
        sca_source_commit_dtm,
        sca_source_update_dtm,
        ss_cd,
        global_name,
        sales_rep_number,
        ep_sk_salesrep_id_int,
        sales_territory_key,
        ep_sk_territory_id_int,
        ru_sales_order_line_key,
        bk_sales_credit_type_code,
        ep_sk_sales_credit_type_id_int,
        sca_source_type_cd
    FROM {{ source('raw', 'wi_sca_retro_nrt_1') }}
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
        ep_source_line_id_int,
        sca_source_commit_dtm,
        sca_source_update_dtm,
        ss_cd,
        global_name,
        sales_rep_number,
        new_sales_rep_number,
        ep_sk_salesrep_id_int,
        sales_territory_key,
        new_sales_territory_key,
        ep_sk_territory_id_int,
        ru_sales_order_line_key,
        new_ru_sales_order_line_key,
        bk_sales_credit_type_code,
        new_bk_sales_credit_type_code,
        ep_sk_sales_credit_type_id_int,
        sca_source_type_cd
    FROM source_n_sca_for_all_trx_nrt_hist_tv
)

SELECT * FROM final
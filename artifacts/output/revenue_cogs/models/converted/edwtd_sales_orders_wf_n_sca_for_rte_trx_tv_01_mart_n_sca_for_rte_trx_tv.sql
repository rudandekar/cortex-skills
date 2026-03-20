{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sca_for_rte_trx_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SCA_FOR_RTE_TRX_TV',
        'target_table': 'N_SCA_FOR_RTE_TRX_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.360072+00:00'
    }
) }}

WITH 

source_n_sca_for_rte_trx_tv AS (
    SELECT
        sales_credit_assignment_key,
        ss_cd,
        sk_line_id_int,
        sk_line_seq_id_int,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sca_source_type_cd,
        sca_sales_commission_pct,
        sales_credit_type_cd,
        sales_rep_num,
        sales_territory_key,
        ep_transaction_id_int,
        start_tv_dt,
        end_tv_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_sls_terr_assignment_type_cd
    FROM {{ source('raw', 'n_sca_for_rte_trx_tv') }}
),

source_w_sca_for_rte_trx AS (
    SELECT
        sales_credit_assignment_key,
        ss_cd,
        sk_line_id_int,
        sk_line_seq_id_int,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sca_source_type_cd,
        sca_sales_commission_pct,
        sales_credit_type_cd,
        sales_rep_num,
        sales_territory_key,
        ep_transaction_id_int,
        start_tv_dt,
        end_tv_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        bk_sls_terr_assignment_type_cd
    FROM {{ source('raw', 'w_sca_for_rte_trx') }}
),

final AS (
    SELECT
        sales_credit_assignment_key,
        ss_cd,
        sk_line_id_int,
        sk_line_seq_id_int,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sca_source_type_cd,
        sca_sales_commission_pct,
        sales_credit_type_cd,
        sales_rep_num,
        sales_territory_key,
        ep_transaction_id_int,
        start_tv_dt,
        end_tv_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_sls_terr_assignment_type_cd
    FROM source_w_sca_for_rte_trx
)

SELECT * FROM final
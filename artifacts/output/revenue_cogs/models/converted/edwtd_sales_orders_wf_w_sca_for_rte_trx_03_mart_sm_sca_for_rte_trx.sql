{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sca_for_rte_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SCA_FOR_RTE_TRX',
        'target_table': 'SM_SCA_FOR_RTE_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.342933+00:00'
    }
) }}

WITH 

source_ex_xxotm_phx_trx_sc_rt AS (
    SELECT
        trx_sc_id,
        trx_id,
        sc_id,
        sc_sequence,
        trx_split_id,
        total_split_percent,
        sc_split_percent,
        trx_split_percent,
        latest_sc_flag,
        parent_child_flag,
        otm_batch_id,
        object_version_number,
        sc_duplicate_flag,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        start_date,
        expiration_date,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        bk_sls_terr_assignment_type_cd
    FROM {{ source('raw', 'ex_xxotm_phx_trx_sc_rt') }}
),

source_sm_sca_for_rte_trx AS (
    SELECT
        sales_credit_assignment_key,
        ss_cd,
        sk_line_seq_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_sca_for_rte_trx') }}
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
        sk_line_seq_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_w_sca_for_rte_trx
)

SELECT * FROM final
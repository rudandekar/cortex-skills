{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_sls_crdts_rbk_mnl_trx_dr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_SLS_CRDTS_RBK_MNL_TRX_DR',
        'target_table': 'ST_OTM_SLS_CRDT_RBK_MNL_TRX_DR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.373476+00:00'
    }
) }}

WITH 

source_st_otm_sls_crdts_rbk_mnl_trx AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        debit_credit_code,
        etl_process_dtm,
        last_update_date,
        latest_sc_flag,
        order_line_id,
        otm_terr_id,
        otm_terr_name,
        otm_terr_type_code,
        rebok_channel_flag,
        salesrep_number,
        sc_assignment_mode_code,
        sc_last_upd_date,
        sc_reason_code,
        sc_split_percent,
        sc_type_code,
        sc_unallocated,
        sc_usage_code,
        shr_node_id,
        territory_type_code,
        trx_orig_code,
        trx_sc_id,
        trx_source_type,
        user_name,
        rollup_flag,
        sc_id,
        sc_duplicate_flag,
        change_type_code,
        source_commit_time,
        ucrm_case_num
    FROM {{ source('raw', 'st_otm_sls_crdts_rbk_mnl_trx') }}
),

final AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        debit_credit_code,
        etl_process_dtm,
        last_update_date,
        latest_sc_flag,
        order_line_id,
        otm_terr_id,
        otm_terr_name,
        otm_terr_type_code,
        rebok_channel_flag,
        salesrep_number,
        sc_assignment_mode_code,
        sc_last_upd_date,
        sc_reason_code,
        sc_split_percent,
        sc_type_code,
        sc_unallocated,
        sc_usage_code,
        shr_node_id,
        territory_type_code,
        trx_orig_code,
        trx_sc_id,
        trx_source_type,
        user_name,
        rollup_flag,
        sc_id,
        sc_duplicate_flag,
        change_type_code,
        trx_version_change_flg,
        distributor_bucket_flg,
        source_commit_time,
        ucrm_case_num
    FROM source_st_otm_sls_crdts_rbk_mnl_trx
)

SELECT * FROM final
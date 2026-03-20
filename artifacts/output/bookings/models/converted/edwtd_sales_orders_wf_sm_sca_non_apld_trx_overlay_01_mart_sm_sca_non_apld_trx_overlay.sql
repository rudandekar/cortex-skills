{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_sca_non_apld_trx_overlay', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_SCA_NON_APLD_TRX_OVERLAY',
        'target_table': 'SM_SCA_NON_APLD_TRX_OVERLAY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.257342+00:00'
    }
) }}

WITH 

source_st_otm_sca_napld_trx_oly_cx AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        cust_trx_line_id,
        debit_credit_code,
        etl_process_dtm,
        global_name,
        last_update_date,
        latest_sc_flag,
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
        trx_split_sc_id,
        user_name,
        ucrm_case_num,
        split_1_bu_key,
        ep_trx_split_sc_id,
        bk_sls_terr_assignment_type_cd
    FROM {{ source('raw', 'st_otm_sca_napld_trx_oly_cx') }}
),

source_st_otm_sls_crdts_napld AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        cust_trx_line_id,
        debit_credit_code,
        etl_process_dtm,
        global_name,
        last_update_date,
        latest_sc_flag,
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
        trx_sc_id,
        user_name,
        ucrm_case_num,
        split_1_bu_key,
        trx_split_sc_id
    FROM {{ source('raw', 'st_otm_sls_crdts_napld') }}
),

final AS (
    SELECT
        sales_cr_assgn_nonappld_key,
        sk_trx_sc_id_int,
        ep_trx_split_sc_id,
        bk_sls_terr_assignment_type_cd,
        edw_create_dtm,
        edw_create_user
    FROM source_st_otm_sls_crdts_napld
)

SELECT * FROM final
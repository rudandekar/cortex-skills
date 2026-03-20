{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sca_apld_trx_direct_rebok', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SCA_APLD_TRX_DIRECT_REBOK',
        'target_table': 'W_SCA_APLD_TRX_DIRECT_REBOK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.537873+00:00'
    }
) }}

WITH 

source_ex_otm_sls_crdts_apld AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        debit_credit_code,
        etl_process_dtm,
        exception_type,
        global_name,
        last_update_date,
        latest_sc_flag,
        order_line_id,
        otm_terr_id,
        otm_terr_name,
        otm_terr_type_code,
        rebok_channel_flag,
        salesrep_number,
        sc_assignment_mode_code,
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
    FROM {{ source('raw', 'ex_otm_sls_crdts_apld') }}
),

source_st_otm_sls_crdts_apld AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        debit_credit_code,
        etl_process_dtm,
        global_name,
        last_update_date,
        latest_sc_flag,
        order_line_id,
        otm_terr_id,
        otm_terr_name,
        otm_terr_type_code,
        rebok_channel_flag,
        salesrep_number,
        sc_assignment_mode_code,
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
    FROM {{ source('raw', 'st_otm_sls_crdts_apld') }}
),

final AS (
    SELECT
        sales_credit_assgn_appld_key,
        rebok_channel_flg,
        src_rptd_rebok_otm_terr_id_int,
        src_rptd_rebok_otm_terr_name,
        src_rptd_rebok_otm_terr_typ_cd,
        sk_trx_sc_id_int,
        pd_assignment_mode_cd,
        pd_scaa_creation_dt,
        pd_sales_credit_type_cd,
        pd_sales_rep_num,
        pd_sales_territory_key,
        pd_sales_order_line_key,
        pd_sales_commission_pct,
        pd_sca_source_type_cd,
        pd_ss_cd,
        pd_created_by_erp_user_name,
        bk_sls_crdt_asgnmnt_reason_cd,
        pd_sls_credit_unallocated_flg,
        pd_sales_credit_usage_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        action_code,
        dml_type,
        pd_sls_credit_last_update_dtm,
        ucrm_case_num
    FROM source_st_otm_sls_crdts_apld
)

SELECT * FROM final
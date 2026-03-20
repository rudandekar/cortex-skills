{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sca_apld_trx_direct_rebok', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SCA_APLD_TRX_DIRECT_REBOK',
        'target_table': 'ST_OTM_SLS_CRDTS_APLD_DR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.571270+00:00'
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
        ucrm_case_num
    FROM source_st_otm_sls_crdts_apld
)

SELECT * FROM final
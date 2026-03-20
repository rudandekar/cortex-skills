{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sca_non_apld_trx_oly_rtnr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SCA_NON_APLD_TRX_OLY_RTNR',
        'target_table': 'ST_OTM_SCA_NAPLD_TRX_OLY_RTNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.993422+00:00'
    }
) }}

WITH 

source_ex_otm_sca_napld_trx_oly_rtnr AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        cust_trx_line_id,
        debit_credit_code,
        etl_process_dtm,
        exception_type,
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
        trx_split_bu_key,
        trx_split_id,
        split_1_bu_key,
        split_1_percentage,
        split_2_bu_key,
        split_2_percentage,
        total_split
    FROM {{ source('raw', 'ex_otm_sca_napld_trx_oly_rtnr') }}
),

source_st_otm_sca_napld_trx_oly_rtnr AS (
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
        trx_split_bu_key,
        trx_split_id,
        split_1_bu_key,
        split_1_percentage,
        split_2_bu_key,
        split_2_percentage,
        total_split
    FROM {{ source('raw', 'st_otm_sca_napld_trx_oly_rtnr') }}
),

final AS (
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
        trx_split_bu_key,
        trx_split_id,
        split_1_bu_key,
        split_1_percentage,
        split_2_bu_key,
        split_2_percentage,
        total_split
    FROM source_st_otm_sca_napld_trx_oly_rtnr
)

SELECT * FROM final
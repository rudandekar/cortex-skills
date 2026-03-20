{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_sls_crdts_pos', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_SLS_CRDTS_POS',
        'target_table': 'ST_OTM_SLS_CRDTS_POS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.169477+00:00'
    }
) }}

WITH 

source_ff_otm_sls_crdts_pos AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        debit_credit_code,
        global_name,
        last_update_date,
        latest_sc_flag,
        otm_terr_id,
        otm_terr_name,
        otm_terr_type_code,
        pos_trans_id,
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
        trx_source_type,
        user_name,
        wips_cost_price,
        wips_list_price,
        wips_unit_price,
        sk3_sc_id_int,
        last_updated_dtm,
        dv_last_updated_dt,
        latest_sales_credit_flg,
        source_deleted_flg,
        sales_credit_duplicate_flg,
        ucrm_case_num
    FROM {{ source('raw', 'ff_otm_sls_crdts_pos') }}
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
        otm_terr_id,
        otm_terr_name,
        otm_terr_type_code,
        pos_trans_id,
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
        trx_source_type,
        user_name,
        wips_cost_price,
        wips_list_price,
        wips_unit_price,
        sk3_sc_id_int,
        last_updated_dtm,
        dv_last_updated_dt,
        latest_sales_credit_flg,
        source_deleted_flg,
        sales_credit_duplicate_flg,
        ucrm_case_num,
        split_1_bu_key,
        trx_split_sc_id
    FROM source_ff_otm_sls_crdts_pos
)

SELECT * FROM final
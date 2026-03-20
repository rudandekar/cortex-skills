{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_sls_crdts_apld', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_SLS_CRDTS_APLD',
        'target_table': 'ST_OTM_SLS_CRDTS_APLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.556287+00:00'
    }
) }}

WITH 

source_ff_otm_sls_crdts_apld AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        debit_credit_code,
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
        ucrm_case_num,
        action_cd,
        create_datetime,
        sc_duplicate_flag
    FROM {{ source('raw', 'ff_otm_sls_crdts_apld') }}
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
        ucrm_case_num,
        split_1_bu_key,
        trx_split_sc_id
    FROM source_ff_otm_sls_crdts_apld
)

SELECT * FROM final
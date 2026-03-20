{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_sls_crdts_napld_dr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_SLS_CRDTS_NAPLD_DR',
        'target_table': 'ST_OTM_SLS_CRDTS_NAPLD_DR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.774031+00:00'
    }
) }}

WITH 

source_st_otm_sls_crdts_napld_dr AS (
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
        ucrm_case_num
    FROM {{ source('raw', 'st_otm_sls_crdts_napld_dr') }}
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
        trx_sc_id,
        user_name,
        ucrm_case_num
    FROM source_st_otm_sls_crdts_napld_dr
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_sls_crdt_rte_trx_oly_rtnr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_SLS_CRDT_RTE_TRX_OLY_RTNR',
        'target_table': 'ST_OTM_SLS_CRDT_RTE_TRX_OLY_RTNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.135485+00:00'
    }
) }}

WITH 

source_st_otm_sls_crdt_rte_trx_oly_rtnr AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        debit_credit_code,
        etl_process_dtm,
        last_update_date,
        latest_sc_flag,
        source_trx_id,
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
        total_split,
        split_2_type
    FROM {{ source('raw', 'st_otm_sls_crdt_rte_trx_oly_rtnr') }}
),

final AS (
    SELECT
        cdb_dequeue_time,
        creation_date,
        debit_credit_code,
        etl_process_dtm,
        last_update_date,
        latest_sc_flag,
        source_trx_id,
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
        total_split,
        split_2_type
    FROM source_st_otm_sls_crdt_rte_trx_oly_rtnr
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_pos_sca_adj_chng_oly_rtnr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_POS_SCA_ADJ_CHNG_OLY_RTNR',
        'target_table': 'SM_POS_SCA_ADJ_CHNG_OLY_RTNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.778466+00:00'
    }
) }}

WITH 

source_st_otm_sls_crdts_pos_oly_rtnr AS (
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
        trx_split_sc_id,
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
        trx_version_change_flg,
        distributor_bucket_flg,
        start_date,
        expiration_date,
        ucrm_case_num,
        trx_split_bu_key,
        trx_split_id,
        split_1_bu_key,
        split_1_percentage,
        split_2_bu_key,
        split_2_percentage,
        total_split
    FROM {{ source('raw', 'st_otm_sls_crdts_pos_oly_rtnr') }}
),

final AS (
    SELECT
        pos_scaac_key,
        sk_trx_split_sc_id_int,
        sk_net_change_credits_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_st_otm_sls_crdts_pos_oly_rtnr
)

SELECT * FROM final
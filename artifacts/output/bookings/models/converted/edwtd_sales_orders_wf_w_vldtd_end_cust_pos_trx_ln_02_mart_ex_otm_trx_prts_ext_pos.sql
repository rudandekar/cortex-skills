{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_vldtd_end_cust_pos_trx_ln', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_VLDTD_END_CUST_POS_TRX_LN',
        'target_table': 'EX_OTM_TRX_PRTS_EXT_POS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.811606+00:00'
    }
) }}

WITH 

source_ex_otm_trx_prts_ext_pos AS (
    SELECT
        action_cd,
        assignment_code,
        assignment_source_system,
        batch_id,
        create_datetime,
        creation_date,
        enrichment_code,
        enrichment_party_type,
        exception_type,
        expiration_date,
        hz_cr_party_id,
        last_update_date,
        pos_trans_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_party_ext_id,
        user_name,
        validation_date,
        ucrm_case_num
    FROM {{ source('raw', 'ex_otm_trx_prts_ext_pos') }}
),

source_st_otm_trx_prts_ext_pos AS (
    SELECT
        action_cd,
        assignment_code,
        assignment_source_system,
        batch_id,
        create_datetime,
        creation_date,
        enrichment_code,
        enrichment_party_type,
        expiration_date,
        hz_cr_party_id,
        last_update_date,
        pos_trans_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_party_ext_id,
        user_name,
        validation_date,
        ucrm_case_num
    FROM {{ source('raw', 'st_otm_trx_prts_ext_pos') }}
),

final AS (
    SELECT
        action_cd,
        assignment_code,
        assignment_source_system,
        batch_id,
        create_datetime,
        creation_date,
        enrichment_code,
        enrichment_party_type,
        exception_type,
        expiration_date,
        hz_cr_party_id,
        last_update_date,
        pos_trans_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_party_ext_id,
        user_name,
        validation_date,
        ucrm_case_num
    FROM source_st_otm_trx_prts_ext_pos
)

SELECT * FROM final
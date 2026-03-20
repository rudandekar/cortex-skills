{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_vldtd_end_cst_rbk_mnl_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_VLDTD_END_CST_RBK_MNL_TRX',
        'target_table': 'EX_OTM_TRX_PRTS_EXT_MNL_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.409554+00:00'
    }
) }}

WITH 

source_st_otm_trx_prts_ext_mnl_trx AS (
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
        order_line_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_orig_code,
        trx_party_ext_id,
        trx_source_type,
        user_name,
        validation_date,
        ucrm_case_num
    FROM {{ source('raw', 'st_otm_trx_prts_ext_mnl_trx') }}
),

source_ex_otm_trx_prts_ext_mnl_trx AS (
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
        order_line_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_orig_code,
        trx_party_ext_id,
        trx_source_type,
        user_name,
        validation_date,
        ucrm_case_num
    FROM {{ source('raw', 'ex_otm_trx_prts_ext_mnl_trx') }}
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
        order_line_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_orig_code,
        trx_party_ext_id,
        trx_source_type,
        user_name,
        validation_date,
        ucrm_case_num
    FROM source_ex_otm_trx_prts_ext_mnl_trx
)

SELECT * FROM final
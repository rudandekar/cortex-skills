{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_distributor_claim', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTRIBUTOR_CLAIM',
        'target_table': 'N_DISTRIBUTOR_CLAIM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.195326+00:00'
    }
) }}

WITH 

source_w_distributor_claim AS (
    SELECT
        bk_claim_id_int,
        source_last_update_dtm,
        source_created_dtm,
        source_deleted_flg,
        created_by_as_reported_user,
        updated_by_as_reported_user,
        disti_claim_created_dtm,
        disti_claim_disti_status_cd,
        disti_claim_smplfd_status_cd,
        disti_claim_status_cd,
        bk_promotion_type_cd,
        pos_transaction_type_cd,
        claim_cr_dr_type_cd,
        wips_originator_id_int,
        transactional_currency_cd,
        crtd_by_cisco_worker_party_key,
        upd_by_cisco_worker_party_key,
        bk_disti_claim_group_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        disti_provided_claim_ref_num,
        expired_flg,
        autovalidate_flg
    FROM {{ source('raw', 'w_distributor_claim') }}
),

final AS (
    SELECT
        bk_claim_id_int,
        source_last_update_dtm,
        source_created_dtm,
        source_deleted_flg,
        created_by_as_reported_user,
        updated_by_as_reported_user,
        disti_claim_created_dtm,
        disti_claim_disti_status_cd,
        disti_claim_smplfd_status_cd,
        disti_claim_status_cd,
        bk_promotion_type_cd,
        pos_transaction_type_cd,
        claim_cr_dr_type_cd,
        wips_originator_id_int,
        transactional_currency_cd,
        crtd_by_cisco_worker_party_key,
        upd_by_cisco_worker_party_key,
        bk_disti_claim_group_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        disti_provided_claim_ref_num,
        expired_flg,
        autovalidate_flg
    FROM source_w_distributor_claim
)

SELECT * FROM final
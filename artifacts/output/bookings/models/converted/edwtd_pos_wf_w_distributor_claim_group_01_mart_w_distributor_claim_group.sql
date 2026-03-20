{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_distributor_claim_group', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTRIBUTOR_CLAIM_GROUP',
        'target_table': 'W_DISTRIBUTOR_CLAIM_GROUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.446874+00:00'
    }
) }}

WITH 

source_ex_dca_header AS (
    SELECT
        dca_id,
        oracle_reason_code,
        salesrep_name,
        comments,
        justification,
        dca_status,
        dca_approved_by,
        proxy_approver,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        saf_interface_date,
        group_id,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_dca_header') }}
),

source_st_dca_header AS (
    SELECT
        dca_id,
        oracle_reason_code,
        salesrep_name,
        comments,
        justification,
        dca_status,
        dca_approved_by,
        proxy_approver,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        saf_interface_date,
        group_id,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_dca_header') }}
),

final AS (
    SELECT
        bk_disti_claim_group_id_int,
        sales_rep_name,
        claim_group_group_id_int,
        ar_system_interface_dtm,
        source_last_updated_dtm,
        source_created_dtm,
        source_deleted_flg,
        updated_by_user,
        created_by_user,
        claim_grp_aprvd_prxy_rprtd_usr,
        claim_grp_aprvd_by_rprtd_usr,
        justification_cmt,
        approver_cmt,
        reason_cd,
        bk_disti_claim_group_status_cd,
        claim_grp_aprvd_wrkr_prty_key,
        claim_grp_aprvd_proxy_prty_key,
        crtd_by_cisco_worker_party_key,
        upd_by_cisco_worker_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_dca_header
)

SELECT * FROM final
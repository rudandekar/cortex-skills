{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_pos_sca_adj_chng_overlay_nrt', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_POS_SCA_ADJ_CHNG_OVERLAY_NRT',
        'target_table': 'SM_POS_SCA_ADJ_CHNG_OVERLAY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.433346+00:00'
    }
) }}

WITH 

source_st_xxotm_phx_trx_sc_nrt AS (
    SELECT
        trx_sc_id,
        trx_id,
        sc_id,
        sc_sequence,
        trx_split_id,
        total_split_percent,
        sc_split_percent,
        trx_split_percent,
        latest_sc_flag,
        parent_child_flag,
        otm_batch_id,
        object_version_number,
        sc_duplicate_flag,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        start_date,
        expiration_date,
        source_dml_type,
        source_commit_time
    FROM {{ source('raw', 'st_xxotm_phx_trx_sc_nrt') }}
),

final AS (
    SELECT
        pos_scaac_key,
        sk_trx_sc_id_int,
        ep_trx_split_sc_id,
        bk_sls_terr_assignment_type_cd,
        sk_net_change_credits_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_st_xxotm_phx_trx_sc_nrt
)

SELECT * FROM final
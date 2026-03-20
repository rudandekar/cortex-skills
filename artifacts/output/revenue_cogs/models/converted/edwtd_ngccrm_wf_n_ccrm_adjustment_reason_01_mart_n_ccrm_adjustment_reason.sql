{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_adjustment_reason', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_ADJUSTMENT_REASON',
        'target_table': 'N_CCRM_ADJUSTMENT_REASON',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.139211+00:00'
    }
) }}

WITH 

source_w_ccrm_adjustment_reason AS (
    SELECT
        bk_ccrm_adjustment_reason_cd,
        bk_ccrm_adj_reason_ver_num_int,
        reason_flow_flg,
        reason_path_cd,
        reason_descr,
        sk_reason_id_int,
        accounting_scope_summary_cd,
        ccrm_reason_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_adjustment_reason') }}
),

final AS (
    SELECT
        bk_ccrm_adjustment_reason_cd,
        bk_ccrm_adj_reason_ver_num_int,
        reason_flow_flg,
        reason_path_cd,
        reason_descr,
        sk_reason_id_int,
        accounting_scope_summary_cd,
        ccrm_reason_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_adjustment_reason
)

SELECT * FROM final
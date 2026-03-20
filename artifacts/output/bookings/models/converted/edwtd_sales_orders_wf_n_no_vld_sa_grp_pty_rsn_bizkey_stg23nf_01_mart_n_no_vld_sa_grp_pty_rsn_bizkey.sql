{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_no_vld_sa_grp_pty_rsn_bizkey_stg23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_NO_VLD_SA_GRP_PTY_RSN_BIZKEY_STG23NF',
        'target_table': 'N_NO_VLD_SA_GRP_PTY_RSN_BIZKEY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.288386+00:00'
    }
) }}

WITH 

source_n_no_vld_sa_grp_pty_rsn_bizkey AS (
    SELECT
        no_vld_sls_acct_pty_rsn_name,
        ep_source_cd,
        no_vld_sls_acct_pty_rsn_descr,
        reason_start_dt,
        reason_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_no_vld_sa_grp_pty_rsn_bizkey') }}
),

final AS (
    SELECT
        no_vld_sls_acct_pty_rsn_name,
        ep_source_cd,
        no_vld_sls_acct_pty_rsn_descr,
        reason_start_dt,
        reason_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_no_vld_sa_grp_pty_rsn_bizkey
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_group_member_cust', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_GROUP_MEMBER_CUST',
        'target_table': 'N_EAC_GROUP_MEMBER_CUST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.312791+00:00'
    }
) }}

WITH 

source_w_eac_group_member_cust AS (
    SELECT
        bk_eac_group_id,
        cust_prty_key,
        bk_effective_dt,
        expiration_dt,
        src_last_rptd_uptd_dtm,
        dv_src_last_uptd_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_group_member_cust') }}
),

final AS (
    SELECT
        bk_eac_group_id,
        cust_prty_key,
        bk_effective_dt,
        expiration_dt,
        src_last_rptd_uptd_dtm,
        dv_src_last_uptd_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_eac_group_member_cust
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_eac_group_member_cust', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_W_EAC_GROUP_MEMBER_CUST',
        'target_table': 'W_EAC_GROUP_MEMBER_CUST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.084423+00:00'
    }
) }}

WITH 

source_st_xxfsam_eac_grp_member_cust_v AS (
    SELECT
        eac_group_id,
        customer_party_bu,
        effective_date,
        expiration_date,
        last_update_date
    FROM {{ source('raw', 'st_xxfsam_eac_grp_member_cust_v') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxfsam_eac_grp_member_cust_v
)

SELECT * FROM final
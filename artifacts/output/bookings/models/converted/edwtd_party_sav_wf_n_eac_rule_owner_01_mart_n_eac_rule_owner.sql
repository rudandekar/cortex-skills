{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_rule_owner', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_RULE_OWNER',
        'target_table': 'N_EAC_RULE_OWNER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.615304+00:00'
    }
) }}

WITH 

source_w_eac_rule_owner AS (
    SELECT
        bk_eac_rule_owner_id,
        bk_sls_terr_asgmt_type_cd,
        sales_territory_key,
        bk_sales_rep_num,
        bk_eac_org_id,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        bk_eac_rule_id,
        rule_owner_start_dt,
        rule_owner_end_dt,
        src_created_dtm,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        owner_status_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_rule_owner') }}
),

final AS (
    SELECT
        bk_eac_rule_owner_id,
        bk_sls_terr_asgmt_type_cd,
        sales_territory_key,
        bk_sales_rep_num,
        bk_eac_org_id,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        bk_eac_rule_id,
        rule_owner_start_dt,
        rule_owner_end_dt,
        src_created_dtm,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        owner_status_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_eac_rule_owner
)

SELECT * FROM final
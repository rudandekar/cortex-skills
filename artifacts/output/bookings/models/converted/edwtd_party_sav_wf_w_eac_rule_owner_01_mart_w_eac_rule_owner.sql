{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_eac_rule_owner', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_W_EAC_RULE_OWNER',
        'target_table': 'W_EAC_RULE_OWNER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.130833+00:00'
    }
) }}

WITH 

source_st_xxfsam_eac_rule_owner_v AS (
    SELECT
        rule_id,
        rule_owner_dtl_id,
        rule_owner_id,
        rule_node_id,
        territory_type,
        rule_owner_start_date,
        rule_owner_end_date,
        rule_owner_creation_date,
        rule_owner_created_by,
        rule_owner_last_updated_date,
        rule_owner_last_updated_by,
        organization_name,
        organization_id,
        owner_status
    FROM {{ source('raw', 'st_xxfsam_eac_rule_owner_v') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxfsam_eac_rule_owner_v
)

SELECT * FROM final
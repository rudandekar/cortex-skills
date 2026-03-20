{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_eac_rule_attr', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_W_EAC_RULE_ATTR',
        'target_table': 'W_EAC_RULE_ATTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.533762+00:00'
    }
) }}

WITH 

source_st_xxfsam_eac_rule_header_v AS (
    SELECT
        rule_id,
        rule_start_date,
        rule_end_date,
        eac_group_id,
        rule_header_creation_date,
        rule_header_created_by,
        rule_header_last_updated_by,
        rule_header_last_updated_date,
        rule_detail_id,
        attribute_name,
        attribute_value,
        start_date,
        end_date,
        rule_attr_created_by,
        rule_attr_creation_date,
        rule_attr_last_update_date,
        rule_attr_last_updated_by,
        organization_name,
        organization_id,
        header_status,
        rule_status,
        attribute_status,
        attribute_value_status,
        profile_id,
        profile_name,
        publish_indicator
    FROM {{ source('raw', 'st_xxfsam_eac_rule_header_v') }}
),

final AS (
    SELECT
        bk_eac_rule_attirbute_id,
        bk_eac_rule_id,
        bk_eac_org_id,
        attribute_name,
        attribute_value_name,
        attribute_start_dt,
        attribute_end_dt,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        src_created_dtm,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        attribute_status_name,
        attribute_value_status_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxfsam_eac_rule_header_v
)

SELECT * FROM final
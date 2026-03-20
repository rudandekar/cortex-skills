{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_rule_attr', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_RULE_ATTR',
        'target_table': 'N_EAC_RULE_ATTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.430848+00:00'
    }
) }}

WITH 

source_w_eac_rule_attr AS (
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
    FROM {{ source('raw', 'w_eac_rule_attr') }}
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
        edw_update_user
    FROM source_w_eac_rule_attr
)

SELECT * FROM final
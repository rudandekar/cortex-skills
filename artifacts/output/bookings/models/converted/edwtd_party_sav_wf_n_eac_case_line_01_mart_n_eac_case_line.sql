{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_case_line', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_CASE_LINE',
        'target_table': 'N_EAC_CASE_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.758047+00:00'
    }
) }}

WITH 

source_w_eac_case_line AS (
    SELECT
        bk_eac_case_line_id,
        lst_uptd_by_csco_wrkr_prty_key,
        bk_eac_rule_id,
        bk_eac_group_id,
        bk_eac_case_id,
        change_attribute_type_cd,
        change_attribute_value,
        src_lst_uptd_crtd_dtm,
        dv_src_lst_uptd_dt,
        src_created_dtm,
        dv_src_created_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_case_line') }}
),

final AS (
    SELECT
        bk_eac_case_line_id,
        lst_uptd_by_csco_wrkr_prty_key,
        bk_eac_rule_id,
        bk_eac_group_id,
        bk_eac_case_id,
        change_attribute_type_cd,
        change_attribute_value,
        src_lst_uptd_crtd_dtm,
        dv_src_lst_uptd_dt,
        src_created_dtm,
        dv_src_created_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_eac_case_line
)

SELECT * FROM final
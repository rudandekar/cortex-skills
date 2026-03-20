{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_project_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_PROJECT_TV',
        'target_table': 'N_FINANCIAL_PROJECT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.835250+00:00'
    }
) }}

WITH 

source_n_financial_project_tv AS (
    SELECT
        bk_project_code,
        ru_bk_reclass_project_code,
        start_tv_date,
        end_tv_date,
        ru_bk_parent_project_code,
        bk_department_code,
        bk_company_code,
        bk_project_locality_int,
        ru_bk_reclass_proj_lclty_int,
        ru_bk_parent_proj_lclty_int,
        financial_project_project_name,
        financial_project_enabled_flag,
        spending_level_amount,
        financial_project_usage_descr,
        program_manager_party_key,
        reclass_role,
        parent_role,
        ss_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_project_value_num,
        sk_si_flex_struct_id_int,
        bk_fin_proj_category_id_int
    FROM {{ source('raw', 'n_financial_project_tv') }}
),

source_w_financial_project AS (
    SELECT
        bk_project_code,
        ru_bk_reclass_project_code,
        start_tv_date,
        end_tv_date,
        ru_bk_parent_project_code,
        bk_department_code,
        bk_company_code,
        bk_project_locality_int,
        ru_bk_reclass_proj_lclty_int,
        ru_bk_parent_proj_lclty_int,
        financial_project_project_name,
        financial_project_enabled_flag,
        spending_level_amount,
        financial_project_usage_descr,
        program_manager_party_key,
        reclass_role,
        parent_role,
        ss_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_project_value_num,
        sk_si_flex_struct_id_int,
        bk_fin_proj_category_id_int,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_financial_project') }}
),

final AS (
    SELECT
        bk_project_code,
        bk_project_locality_int,
        bk_company_code,
        bk_department_code,
        financial_project_enabled_flag,
        financial_project_project_name,
        financial_project_usage_descr,
        parent_role,
        program_manager_party_key,
        reclass_role,
        ru_bk_parent_project_code,
        ru_bk_parent_proj_lclty_int,
        ru_bk_reclass_proj_lclty_int,
        ru_bk_reclass_project_code,
        sk_project_value_num,
        sk_si_flex_struct_id_int,
        spending_level_amount,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        bk_fin_proj_category_id_int
    FROM source_w_financial_project
)

SELECT * FROM final
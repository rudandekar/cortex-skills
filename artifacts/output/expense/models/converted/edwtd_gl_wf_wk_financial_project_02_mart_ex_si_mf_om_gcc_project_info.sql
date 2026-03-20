{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_project', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_PROJECT',
        'target_table': 'EX_SI_MF_OM_GCC_PROJECT_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.681832+00:00'
    }
) }}

WITH 

source_st_om_gl_code_combinations AS (
    SELECT
        batch_id,
        code_combination_id,
        global_name,
        account_type,
        chart_of_accounts_id,
        enabled_flag,
        preserve_flag,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        start_date_active,
        end_date_active,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_gl_code_combinations') }}
),

source_st_si_project_info AS (
    SELECT
        batch_id,
        project_value,
        project_name,
        department_value,
        enabled_flag,
        si_flex_struct_id,
        parent_project_value,
        program_manager,
        project_category_id,
        reclass_project_value,
        spending_level,
        usage_description,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_project_info') }}
),

source_st_mf_gl_code_combinations AS (
    SELECT
        batch_id,
        code_combination_id,
        global_name,
        account_type,
        chart_of_accounts_id,
        enabled_flag,
        preserve_flag,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        start_date_active,
        end_date_active,
        ges_update_date,
        create_datetime,
        action_code,
        last_update_date
    FROM {{ source('raw', 'st_mf_gl_code_combinations') }}
),

transformed_exp_w_financial_project AS (
    SELECT
    batch_id,
    bk_project_code,
    ru_bk_reclass_project_code,
    start_date_active,
    end_date_active,
    ru_bk_parent_project_code,
    bk_department_code,
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
    sk_project_value_num,
    sk_si_flex_struct_id_int,
    bk_company_code,
    rank_index,
    dml_type,
    bk_fin_proj_category_id_int,
    action_code,
    IFF(ISNULL(BK_PROJECT_LOCALITY_INT),'NN','NE') AS error_check
    FROM source_st_mf_gl_code_combinations
),

transformed_exp_ex_si_mf_om_gcc_project_info AS (
    SELECT
    batch_id,
    project_value,
    project_name,
    department_value,
    enabled_flag,
    si_flex_struct_id,
    parent_project_value,
    program_manager,
    project_category_id,
    reclass_project_value,
    spending_level,
    usage_description,
    ges_update_date,
    create_datetime,
    action_code,
    exception_type
    FROM transformed_exp_w_financial_project
),

filtered_fil_w_financial_project AS (
    SELECT *
    FROM transformed_exp_ex_si_mf_om_gcc_project_info
    WHERE ERROR_CHECK='NE'
),

filtered_fil_ex_si_mf_om_gcc_project_info AS (
    SELECT *
    FROM filtered_fil_w_financial_project
    WHERE ERROR_CHECK='NN'
),

final AS (
    SELECT
        batch_id,
        project_value,
        project_name,
        department_value,
        enabled_flag,
        si_flex_struct_id,
        parent_project_value,
        program_manager,
        project_category_id,
        reclass_project_value,
        spending_level,
        usage_description,
        last_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM filtered_fil_ex_si_mf_om_gcc_project_info
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_chart_of_accounts', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_CHART_OF_ACCOUNTS',
        'target_table': 'EX_MF_FND_ID_FLEX_STRUC_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.667607+00:00'
    }
) }}

WITH 

source_st_cg1_fnd_id_flex_segments AS (
    SELECT
        application_id,
        id_flex_code,
        id_flex_num,
        application_column_name,
        segment_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        segment_num,
        application_column_index_flag,
        enabled_flag,
        required_flag,
        display_flag,
        display_size,
        security_enabled_flag,
        maximum_description_len,
        concatenation_description_len,
        last_update_login,
        flex_value_set_id,
        range_code,
        default_type,
        default_value,
        runtime_property_function,
        additional_where_clause,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_fnd_id_flex_segments') }}
),

source_st_cg1_fnd_id_flex_struc_tl AS (
    SELECT
        batch_id,
        application_id,
        id_flex_code,
        id_flex_num,
        language_code,
        last_update_date,
        last_updated_by,
        global_name,
        id_flex_structure_name,
        description,
        creation_date,
        created_by,
        last_update_login,
        shorthand_prompt,
        source_lang,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_fnd_id_flex_struc_tl') }}
),

source_n_fin_subacct_locality_tv AS (
    SELECT
        bk_subaccount_locality_int,
        start_tv_date,
        end_tv_date,
        subaccount_locality_name,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_si_flex_struct_id_int
    FROM {{ source('raw', 'n_fin_subacct_locality_tv') }}
),

source_n_fin_acct_locality_tv AS (
    SELECT
        start_tv_date,
        end_tv_date,
        financial_acct_locality_name,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_si_flex_struct_id_int,
        bk_fin_acct_locality_int
    FROM {{ source('raw', 'n_fin_acct_locality_tv') }}
),

source_st_mf_fnd_id_flex_segments AS (
    SELECT
        batch_id,
        application_id,
        id_flex_code,
        id_flex_num,
        application_column_name,
        global_name,
        flex_value_set_id,
        segment_name,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fnd_id_flex_segments') }}
),

source_st_mf_fnd_id_flex_struc_tl AS (
    SELECT
        batch_id,
        application_id,
        id_flex_code,
        id_flex_num,
        language_code,
        global_name,
        id_flex_structure_name,
        description,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fnd_id_flex_struc_tl') }}
),

source_n_fin_proj_locality_tv AS (
    SELECT
        bk_project_locality_int,
        start_tv_date,
        end_tv_date,
        project_locality_name,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        sk_si_flex_struct_id_int
    FROM {{ source('raw', 'n_fin_proj_locality_tv') }}
),

final AS (
    SELECT
        batch_id,
        application_id,
        id_flex_code,
        id_flex_num,
        language_code,
        global_name,
        id_flex_structure_name,
        description,
        proj_flex_value_set_id,
        acct_flex_value_set_id,
        subacct_flex_value_set_id,
        create_datetime,
        action_code,
        exception_type
    FROM source_n_fin_proj_locality_tv
)

SELECT * FROM final
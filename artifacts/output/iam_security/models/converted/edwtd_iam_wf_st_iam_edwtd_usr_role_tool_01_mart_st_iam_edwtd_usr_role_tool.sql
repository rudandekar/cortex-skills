{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_iam_edwtd_usr_role_tool', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_ST_IAM_EDWTD_USR_ROLE_TOOL',
        'target_table': 'ST_IAM_EDWTD_USR_ROLE_TOOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.725679+00:00'
    }
) }}

WITH 

source_ff_iam_edwtd_usr_role_tool AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        assigned_app_delim_list,
        user_role_expt_flag,
        proxy_flag,
        grantor_universal_id,
        grantor_cec_id,
        grantor_cco_id,
        status,
        tr_flag,
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'ff_iam_edwtd_usr_role_tool') }}
),

final AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        assigned_app_delim_list,
        user_role_expt_flag,
        proxy_flag,
        grantor_universal_id,
        grantor_cec_id,
        grantor_cco_id,
        status,
        tr_flag,
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM source_ff_iam_edwtd_usr_role_tool
)

SELECT * FROM final
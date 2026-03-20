{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_usr_role_biz_attr_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_USR_ROLE_BIZ_ATTR_MAP',
        'target_table': 'EX_IAM_EDWTD_USR_ROLE_BIZ_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.787432+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_role_biz_data AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        biz_data_attrib_name,
        biz_data_attrib_descr,
        excl_restr_flag,
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
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_biz_data') }}
),

final AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        biz_data_attrib_name,
        biz_data_attrib_descr,
        excl_restr_flag,
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
        create_datetime,
        exception_type
    FROM source_st_iam_edwtd_usr_role_biz_data
)

SELECT * FROM final
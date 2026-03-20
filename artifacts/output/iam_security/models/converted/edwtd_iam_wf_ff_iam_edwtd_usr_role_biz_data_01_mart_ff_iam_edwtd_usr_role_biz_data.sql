{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_usr_role_biz_data', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_USR_ROLE_BIZ_DATA',
        'target_table': 'FF_IAM_EDWTD_USR_ROLE_BIZ_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.768902+00:00'
    }
) }}

WITH 

source_iam_edwtd_usr_role_biz_data_vw AS (
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
        update_date
    FROM {{ source('raw', 'iam_edwtd_usr_role_biz_data_vw') }}
),

transformed_exptrans AS (
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
    'I' AS action_code,
    'BatchId' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_edwtd_usr_role_biz_data_vw
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
        create_datetime
    FROM transformed_exptrans
)

SELECT * FROM final
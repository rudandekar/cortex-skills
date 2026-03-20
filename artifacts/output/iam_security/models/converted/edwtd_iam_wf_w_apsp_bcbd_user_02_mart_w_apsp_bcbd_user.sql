{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_apsp_bcbd_user', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_APSP_BCBD_USER',
        'target_table': 'W_APSP_BCBD_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.747938+00:00'
    }
) }}

WITH 

source_wi_user_att_load AS (
    SELECT
        user_id,
        theater_name,
        user_group
    FROM {{ source('raw', 'wi_user_att_load') }}
),

source_w_apsp_bcbd_user AS (
    SELECT
        bcbd_user_key,
        bcbd_user_type,
        ru_iam_role_name,
        ru_iam_user_key,
        ru_apsp_bcbd_user_group_name,
        ru_apsp_bcbd_theater_name,
        ru_bcbd_mailer_alias_addr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_bcbd_user') }}
),

final AS (
    SELECT
        bcbd_user_key,
        bcbd_user_type,
        ru_iam_role_name,
        ru_iam_user_key,
        ru_apsp_bcbd_user_group_name,
        ru_apsp_bcbd_theater_name,
        ru_bcbd_mailer_alias_addr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM source_w_apsp_bcbd_user
)

SELECT * FROM final
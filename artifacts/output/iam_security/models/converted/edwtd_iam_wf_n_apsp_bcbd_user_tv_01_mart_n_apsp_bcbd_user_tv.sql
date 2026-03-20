{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_apsp_bcbd_user_tv', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_APSP_BCBD_USER_TV',
        'target_table': 'N_APSP_BCBD_USER_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.720719+00:00'
    }
) }}

WITH 

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
        end_tv_dt
    FROM source_w_apsp_bcbd_user
)

SELECT * FROM final
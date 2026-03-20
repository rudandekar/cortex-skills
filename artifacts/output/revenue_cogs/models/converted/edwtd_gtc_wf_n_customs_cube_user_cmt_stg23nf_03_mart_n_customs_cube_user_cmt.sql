{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_customs_cube_user_cmt_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_CUSTOMS_CUBE_USER_CMT_STG23NF',
        'target_table': 'N_CUSTOMS_CUBE_USER_CMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.081720+00:00'
    }
) }}

WITH 

source_n_customs_cube_user_cmt AS (
    SELECT
        customs_cube_user_cmt_key,
        bk_cmt_dtm,
        cmtr_cb_usr_cso_wkr_pty_key,
        cmte_cb_usr_cso_wkr_pty_key,
        user_cmt,
        sk_internal_comments_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_customs_cube_user_cmt') }}
),

source_st_xxcfi_cb_internal_comments AS (
    SELECT
        batch_id,
        internal_comments_id,
        identifier1,
        identifier2,
        comments,
        created_by,
        created_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcfi_cb_internal_comments') }}
),

source_sm_user_comments AS (
    SELECT
        user_comment_key,
        sk_internal_comments_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_user_comments') }}
),

final AS (
    SELECT
        customs_cube_user_cmt_key,
        bk_cmt_dtm,
        cmtr_cb_usr_cso_wkr_pty_key,
        cmte_cb_usr_cso_wkr_pty_key,
        user_cmt,
        sk_internal_comments_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_sm_user_comments
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_hts_user_comment_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_HTS_USER_COMMENT_STG23NF',
        'target_table': 'SM_USER_COMMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.085074+00:00'
    }
) }}

WITH 

source_n_hts_user_comment AS (
    SELECT
        hts_user_key_comment_key,
        bk_hrmnzd_tariff_schedule_cd,
        bk_hts_effective_start_dt,
        bk_iso_country_cd,
        bk_active_flg,
        cisco_worker_party_key,
        bk_cmt_dtm,
        user_cmt,
        sk_internal_comments_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_hts_user_comment') }}
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
        user_comment_key,
        sk_internal_comments_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_user_comments
)

SELECT * FROM final
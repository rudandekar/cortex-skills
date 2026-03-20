{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_hts_clsfctn_rule_user_cmt_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_HTS_CLSFCTN_RULE_USER_CMT_STG23NF',
        'target_table': 'SM_USER_COMMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.316035+00:00'
    }
) }}

WITH 

source_n_hts_clsfctn_rule_user_cmt AS (
    SELECT
        hts_clsfctn_rule_user_cmt_key,
        bk_cmt_dtm,
        bk_cube_user_csc_wrkr_prty_key,
        user_cmt,
        sk_internal_comments_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        hts_classification_rule_key
    FROM {{ source('raw', 'n_hts_clsfctn_rule_user_cmt') }}
),

source_ex_xxcfi_cb_internal_comments AS (
    SELECT
        batch_id,
        internal_comments_id,
        identifier1,
        identifier2,
        comments,
        created_by,
        created_date,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_internal_comments') }}
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
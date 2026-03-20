{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_n_itm_cg_hts_clfctn_usr_cmt_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_ITM_CG_HTS_CLFCTN_USR_CMT_STG23NF',
        'target_table': 'EX_XXCFI_CB_CLSFN_INTRNL_CMTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.545266+00:00'
    }
) }}

WITH 

source_sm_itm_cg_hts_clfctn_usr_cmt AS (
    SELECT
        itm_cg_hts_clsfctn_usr_cmt_key,
        sk_internal_comments_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_itm_cg_hts_clfctn_usr_cmt') }}
),

source_n_itm_cg_hts_clfctn_usr_cmt AS (
    SELECT
        itm_cg_hts_clsfctn_usr_cmt_key,
        bk_cmt_dtm,
        bk_cube_usr_csco_wrkr_prty_key,
        user_cmt,
        sk_internal_comments_id_int,
        item_cg_hts_clsfctn_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_itm_cg_hts_clfctn_usr_cmt') }}
),

source_ex_xxcfi_cb_clsfn_intrnl_cmts AS (
    SELECT
        batch_id,
        internal_comments_id,
        item_name,
        country_group_code,
        comments,
        created_by,
        created_date,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_clsfn_intrnl_cmts') }}
),

final AS (
    SELECT
        batch_id,
        internal_comments_id,
        item_name,
        country_group_code,
        comments,
        created_by,
        created_date,
        create_datetime,
        action_code,
        exception_type
    FROM source_ex_xxcfi_cb_clsfn_intrnl_cmts
)

SELECT * FROM final
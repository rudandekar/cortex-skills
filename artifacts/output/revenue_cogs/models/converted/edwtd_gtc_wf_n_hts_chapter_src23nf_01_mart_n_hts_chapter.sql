{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_hts_chapter_src23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_HTS_CHAPTER_SRC23NF',
        'target_table': 'N_HTS_CHAPTER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.839857+00:00'
    }
) }}

WITH 

source_n_hts_chapter AS (
    SELECT
        bk_hts_chapter_num,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_hts_chapter') }}
),

source_xxcfi_cb_hts_cntnt_fltr_dtls AS (
    SELECT
        content_filter_details_id,
        content_filter_id,
        hts_chapter_no,
        description,
        is_new,
        created_by,
        created_date,
        modified_by,
        modified_date
    FROM {{ source('raw', 'xxcfi_cb_hts_cntnt_fltr_dtls') }}
),

filtered_fltr_n_hts_chapter_upd AS (
    SELECT *
    FROM source_xxcfi_cb_hts_cntnt_fltr_dtls
    WHERE SOURCE_DELETED_FLG ='Y'
),

lookup_lkp_n_hts_chapter AS (
    SELECT
        a.*,
        b.*
    FROM filtered_fltr_n_hts_chapter_upd a
    LEFT JOIN {{ source('raw', 'n_hts_chapter') }} b
        ON a.in_hts_chapter_no = b.in_hts_chapter_no
),

transformed_exp_n_hts_chapter_insert AS (
    SELECT
    hts_chapter_no,
    IFF(ISNULL(:LKP.LKP_N_HTS_CHAPTER(HTS_CHAPTER_NO)),'I','N') AS insert_flg,
    'N' AS source_deleted_flg,
    CURRENT_TIMESTAMP() AS edw_create_dtm,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM lookup_lkp_n_hts_chapter
),

filtered_fltr_n_hts_chapter AS (
    SELECT *
    FROM transformed_exp_n_hts_chapter_insert
    WHERE INSERT_FLG='I'
),

lookup_lkp_xxcfi_cb_hts_cntnt_fltr_dtls AS (
    SELECT
        a.*,
        b.*
    FROM filtered_fltr_n_hts_chapter a
    LEFT JOIN {{ source('raw', 'xxcfi_cb_hts_cntnt_fltr_dtls') }} b
        ON a.in_hts_chapter_no = b.in_hts_chapter_no
),

transformed_exp_n_hts_chapter_update AS (
    SELECT
    bk_hts_chapter_num,
    IFF(ISNULL(:LKP.LKP_XXCFI_CB_HTS_CNTNT_FLTR_DTLS(BK_HTS_CHAPTER_NUM)),'Y','N') AS source_deleted_flg,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM lookup_lkp_xxcfi_cb_hts_cntnt_fltr_dtls
),

update_strategy_upd_n_hts_chapter AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_n_hts_chapter_update
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        bk_hts_chapter_num,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_n_hts_chapter
)

SELECT * FROM final
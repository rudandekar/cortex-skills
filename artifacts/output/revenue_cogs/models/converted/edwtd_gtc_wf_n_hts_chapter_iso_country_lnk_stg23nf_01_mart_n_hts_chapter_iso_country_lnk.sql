{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_hts_chapter_iso_country_lnk_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_HTS_CHAPTER_ISO_COUNTRY_LNK_STG23NF',
        'target_table': 'N_HTS_CHAPTER_ISO_COUNTRY_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.040862+00:00'
    }
) }}

WITH 

source_n_hts_chapter_iso_country_lnk AS (
    SELECT
        bk_hts_chapter_num,
        bk_iso_country_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_hts_chapter_iso_country_lnk') }}
),

final AS (
    SELECT
        bk_hts_chapter_num,
        bk_iso_country_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_hts_chapter_iso_country_lnk
)

SELECT * FROM final
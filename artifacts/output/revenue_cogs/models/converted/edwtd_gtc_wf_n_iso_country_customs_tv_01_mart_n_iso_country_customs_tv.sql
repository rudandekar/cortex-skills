{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iso_country_customs_tv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_ISO_COUNTRY_CUSTOMS_TV',
        'target_table': 'N_ISO_COUNTRY_CUSTOMS_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.008449+00:00'
    }
) }}

WITH 

source_n_iso_country_customs_tv AS (
    SELECT
        bk_iso_country_cd,
        bk_world_country_group_cd,
        override_iso_country_cd,
        iso_country_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm
    FROM {{ source('raw', 'n_iso_country_customs_tv') }}
),

source_n_iso_country_customs AS (
    SELECT
        bk_iso_country_cd,
        override_iso_country_cd,
        bk_world_country_group_cd,
        iso_country_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_iso_country_customs') }}
),

final AS (
    SELECT
        bk_iso_country_cd,
        bk_world_country_group_cd,
        override_iso_country_cd,
        iso_country_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm
    FROM source_n_iso_country_customs
)

SELECT * FROM final
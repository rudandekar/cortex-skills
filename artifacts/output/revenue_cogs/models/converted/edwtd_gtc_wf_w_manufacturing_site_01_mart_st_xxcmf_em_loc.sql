{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_manufacturing_site', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_MANUFACTURING_SITE',
        'target_table': 'ST_XXCMF_EM_LOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.717415+00:00'
    }
) }}

WITH 

source_ex_xxcmf_em_loc AS (
    SELECT
        batch_id,
        site_id,
        location,
        country,
        created_by,
        last_updated_by,
        created_date,
        last_updated_date,
        status,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcmf_em_loc') }}
),

source_st_xxcmf_em_loc AS (
    SELECT
        batch_id,
        site_id,
        location,
        country,
        created_by,
        last_updated_by,
        created_date,
        last_updated_date,
        status,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcmf_em_loc') }}
),

source_w_manufacturing_site AS (
    SELECT
        mftrng_oem_sup_pty_key,
        bk_mftrng_loc_name,
        bk_iso_country_code,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_manufacturing_site') }}
),

final AS (
    SELECT
        batch_id,
        site_id,
        location,
        country,
        created_by,
        last_updated_by,
        created_date,
        last_updated_date,
        status,
        create_datetime,
        action_code
    FROM source_w_manufacturing_site
)

SELECT * FROM final
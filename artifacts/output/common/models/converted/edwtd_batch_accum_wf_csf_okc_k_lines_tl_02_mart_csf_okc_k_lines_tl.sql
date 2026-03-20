{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_okc_k_lines_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_OKC_K_LINES_TL',
        'target_table': 'CSF_OKC_K_LINES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.557884+00:00'
    }
) }}

WITH 

source_stg_okc_k_lines_tl AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        id,
        languag,
        source_lang,
        sfwt_flag,
        name,
        comments,
        item_description,
        block23text,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        oke_boe_description,
        cognomen
    FROM {{ source('raw', 'stg_okc_k_lines_tl') }}
),

source_csf_okc_k_lines_tl AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        id,
        language,
        source_lang,
        sfwt_flag,
        name,
        comments,
        item_description,
        block23text,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        oke_boe_description,
        cognomen
    FROM {{ source('raw', 'csf_okc_k_lines_tl') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    id,
    language,
    source_lang,
    sfwt_flag,
    name,
    comments,
    item_description,
    block23text,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    security_group_id,
    oke_boe_description,
    cognomen
    FROM source_csf_okc_k_lines_tl
),

final AS (
    SELECT
        source_dml_type,
        ges_update_date,
        refresh_datetime,
        id,
        languag,
        source_lang,
        sfwt_flag,
        name,
        comments,
        item_description,
        block23text,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        oke_boe_description,
        cognomen
    FROM transformed_exptrans
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_hts_cntnt_fltr_dtl', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_HTS_CNTNT_FLTR_DTL',
        'target_table': 'FF_XXCFI_CB_HTS_CNTNT_FLTR_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.231053+00:00'
    }
) }}

WITH 

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

transformed_exp_xxcfi_cb_hts_cntnt_fltr_dtl AS (
    SELECT
    content_filter_details_id,
    content_filter_id,
    hts_chapter_no,
    description,
    is_new,
    created_by,
    created_date,
    modified_by,
    modified_date,
    1 AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_hts_cntnt_fltr_dtls
),

final AS (
    SELECT
        batch_id,
        content_filter_details_id,
        content_filter_id,
        hts_chapter_no,
        description,
        is_new,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code
    FROM transformed_exp_xxcfi_cb_hts_cntnt_fltr_dtl
)

SELECT * FROM final
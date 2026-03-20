{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_hts_cntnt_fltr_dtl', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_HTS_CNTNT_FLTR_DTL',
        'target_table': 'ST_XXCFI_CB_HTS_CNTNT_FLTR_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.442608+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_hts_cntnt_fltr_dtl AS (
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
    FROM {{ source('raw', 'ff_xxcfi_cb_hts_cntnt_fltr_dtl') }}
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
    FROM source_ff_xxcfi_cb_hts_cntnt_fltr_dtl
)

SELECT * FROM final
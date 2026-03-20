{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_hts_content_filter', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_HTS_CONTENT_FILTER',
        'target_table': 'ST_XXCFI_CB_HTS_CONTENT_FILTER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.913676+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_hts_content_filter AS (
    SELECT
        batch_id,
        content_filter_id,
        country_id,
        country_code,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcfi_cb_hts_content_filter') }}
),

final AS (
    SELECT
        batch_id,
        content_filter_id,
        country_id,
        country_code,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_hts_content_filter
)

SELECT * FROM final
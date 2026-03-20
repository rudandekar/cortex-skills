{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_xxcfi_cb_internal_comments', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_INTERNAL_COMMENTS',
        'target_table': 'ST_XXCFI_CB_INTERNAL_COMMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.376086+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_internal_comments AS (
    SELECT
        batch_id,
        internal_comments_id,
        identifier1,
        identifier2,
        comments,
        created_by,
        created_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcfi_cb_internal_comments') }}
),

final AS (
    SELECT
        batch_id,
        internal_comments_id,
        identifier1,
        identifier2,
        comments,
        created_by,
        created_date,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_internal_comments
)

SELECT * FROM final
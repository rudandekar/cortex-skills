{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_internal_comments', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_INTERNAL_COMMENTS',
        'target_table': 'FF_XXCFI_CB_INTERNAL_COMMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.756600+00:00'
    }
) }}

WITH 

source_xxcfi_cb_internal_comments AS (
    SELECT
        internal_comments_id,
        identifier1,
        identifier2,
        comments,
        created_by,
        created_date
    FROM {{ source('raw', 'xxcfi_cb_internal_comments') }}
),

transformed_exp_ff_xxcfi_cb_internal_comments AS (
    SELECT
    internal_comments_id,
    identifier1,
    identifier2,
    comments,
    created_by,
    created_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_internal_comments
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
    FROM transformed_exp_ff_xxcfi_cb_internal_comments
)

SELECT * FROM final
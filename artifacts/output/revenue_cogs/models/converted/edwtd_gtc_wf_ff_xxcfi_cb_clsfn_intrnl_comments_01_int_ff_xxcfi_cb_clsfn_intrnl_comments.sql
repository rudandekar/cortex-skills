{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_clsfn_intrnl_comments', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_CLSFN_INTRNL_COMMENTS',
        'target_table': 'FF_XXCFI_CB_CLSFN_INTRNL_COMMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.295749+00:00'
    }
) }}

WITH 

source_xxcfi_cb_clsfn_intrnl_comments AS (
    SELECT
        internal_comments_id,
        item_name,
        country_group_code,
        comments,
        created_by,
        created_date
    FROM {{ source('raw', 'xxcfi_cb_clsfn_intrnl_comments') }}
),

transformed_exp_ff_cb_clsfn_intrnl_comments AS (
    SELECT
    internal_comments_id,
    item_name,
    country_group_code,
    comments,
    created_by,
    created_date,
    1 AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_clsfn_intrnl_comments
),

final AS (
    SELECT
        batch_id,
        internal_comments_id,
        item_name,
        country_group_code,
        comments,
        created_by,
        created_date,
        create_datetime,
        action_code
    FROM transformed_exp_ff_cb_clsfn_intrnl_comments
)

SELECT * FROM final
{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_xxcfi_cb_clsfn_intrnl_comments', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_CLSFN_INTRNL_COMMENTS',
        'target_table': 'ST_XXCFI_CB_CLSFN_INTRNL_CMTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.847414+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_clsfn_intrnl_comments AS (
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
    FROM {{ source('raw', 'ff_xxcfi_cb_clsfn_intrnl_comments') }}
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
    FROM source_ff_xxcfi_cb_clsfn_intrnl_comments
)

SELECT * FROM final
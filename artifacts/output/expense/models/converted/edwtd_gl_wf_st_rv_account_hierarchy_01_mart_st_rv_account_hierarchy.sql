{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_rv_account_hierarchy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_RV_ACCOUNT_HIERARCHY',
        'target_table': 'ST_RV_ACCOUNT_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.132104+00:00'
    }
) }}

WITH 

source_ff_rv_account_hierarchy AS (
    SELECT
        hierarchy_type,
        category_id,
        category,
        category_level1,
        category_level2,
        account_number,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'ff_rv_account_hierarchy') }}
),

final AS (
    SELECT
        hierarchy_type,
        category_id,
        category,
        category_level1,
        category_level2,
        account_number,
        action_code,
        batch_id,
        create_datetime
    FROM source_ff_rv_account_hierarchy
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_rv_account_hierarchy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_RV_ACCOUNT_HIERARCHY',
        'target_table': 'FF_RV_ACCOUNT_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.636747+00:00'
    }
) }}

WITH 

source_rv_account_hierarchy AS (
    SELECT
        hierarchy_type,
        category_id,
        category,
        category_level1,
        category_level2,
        account
    FROM {{ source('raw', 'rv_account_hierarchy') }}
),

transformed_exp_rv_account_hierarchy AS (
    SELECT
    hierarchy_type,
    ''BatchId'' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_rv_account_hierarchy
),

final AS (
    SELECT
        hierarchy_type,
        category_id,
        category,
        category_level1,
        category_level2,
        account,
        action_code,
        batch_id,
        create_datetime
    FROM transformed_exp_rv_account_hierarchy
)

SELECT * FROM final
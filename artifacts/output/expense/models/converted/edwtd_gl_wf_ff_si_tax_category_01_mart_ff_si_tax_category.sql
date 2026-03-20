{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_tax_category', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_TAX_CATEGORY',
        'target_table': 'FF_SI_TAX_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.881074+00:00'
    }
) }}

WITH 

source_si_tax_category AS (
    SELECT
        tax_category_id,
        tax_category_value,
        tax_category_desc,
        created_by,
        create_date,
        last_updated_by,
        last_update_date,
        enabled_flag
    FROM {{ source('raw', 'si_tax_category') }}
),

transformed_exp_si_tax_category AS (
    SELECT
    tax_category_id,
    tax_category_value,
    tax_category_desc,
    created_by,
    create_date,
    last_updated_by,
    last_update_date,
    enabled_flag,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_tax_category
),

final AS (
    SELECT
        batch_id,
        tax_category_id,
        tax_category_value,
        tax_category_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_si_tax_category
)

SELECT * FROM final
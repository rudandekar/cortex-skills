{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_tax_category', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_TAX_CATEGORY',
        'target_table': 'ST_SI_TAX_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.155451+00:00'
    }
) }}

WITH 

source_ff_si_tax_category AS (
    SELECT
        batch_id,
        tax_category_id,
        tax_category_value,
        tax_category_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_tax_category') }}
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
    FROM source_ff_si_tax_category
)

SELECT * FROM final
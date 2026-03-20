{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_dist_theatre_and_vat', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_DIST_THEATRE_AND_VAT',
        'target_table': 'WI_COST_VAT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.529953+00:00'
    }
) }}

WITH 

source_wi_dist_theatre AS (
    SELECT
        theater
    FROM {{ source('raw', 'wi_dist_theatre') }}
),

source_wi_cost_vat AS (
    SELECT
        theater,
        cost
    FROM {{ source('raw', 'wi_cost_vat') }}
),

source_wi_ssc_er AS (
    SELECT
        fiscal_month_id,
        theater,
        cost_type,
        sub_cost_type,
        cost
    FROM {{ source('raw', 'wi_ssc_er') }}
),

final AS (
    SELECT
        theater,
        cost
    FROM source_wi_ssc_er
)

SELECT * FROM final
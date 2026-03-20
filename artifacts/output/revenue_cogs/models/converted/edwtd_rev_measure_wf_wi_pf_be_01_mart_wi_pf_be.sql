{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pf_be', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PF_BE',
        'target_table': 'WI_PF_BE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.176292+00:00'
    }
) }}

WITH 

source_wi_pf_be AS (
    SELECT
        hybrid_product_family,
        business_entity_descr,
        pf
    FROM {{ source('raw', 'wi_pf_be') }}
),

final AS (
    SELECT
        hybrid_product_family,
        business_entity_descr,
        pf
    FROM source_wi_pf_be
)

SELECT * FROM final
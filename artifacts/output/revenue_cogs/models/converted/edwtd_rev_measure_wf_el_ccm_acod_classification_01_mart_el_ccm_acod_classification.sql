{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccm_acod_classification', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_CCM_ACOD_CLASSIFICATION',
        'target_table': 'EL_CCM_ACOD_CLASSIFICATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.035492+00:00'
    }
) }}

WITH 

source_twfm022_acod_classification AS (
    SELECT
        id,
        min_acod,
        max_acod,
        created_id,
        created_date,
        updated_id,
        updated_date
    FROM {{ source('raw', 'twfm022_acod_classification') }}
),

final AS (
    SELECT
        id,
        min_acod,
        max_acod,
        created_id,
        created_date,
        updated_id,
        updated_date
    FROM source_twfm022_acod_classification
)

SELECT * FROM final
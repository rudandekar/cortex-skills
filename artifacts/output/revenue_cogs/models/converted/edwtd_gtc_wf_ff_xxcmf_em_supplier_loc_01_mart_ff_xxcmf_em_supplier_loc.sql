{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcmf_em_supplier_loc', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCMF_EM_SUPPLIER_LOC',
        'target_table': 'FF_XXCMF_EM_SUPPLIER_LOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.020548+00:00'
    }
) }}

WITH 

source_xxcmf_em_supplier_loc AS (
    SELECT
        manufacturer_id,
        site_id
    FROM {{ source('raw', 'xxcmf_em_supplier_loc') }}
),

transformed_exptrans AS (
    SELECT
    manufacturer_id,
    site_id,
    'BATCHID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcmf_em_supplier_loc
),

final AS (
    SELECT
        batch_id,
        manufacturer_id,
        site_id,
        create_datetime,
        action_code
    FROM transformed_exptrans
)

SELECT * FROM final
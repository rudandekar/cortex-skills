{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_em_supplier_loc', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_EM_SUPPLIER_LOC',
        'target_table': 'ST_XXCMF_EM_SUPPLIER_LOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.625195+00:00'
    }
) }}

WITH 

source_ff_xxcmf_em_supplier_loc AS (
    SELECT
        batch_id,
        manufacturer_id,
        site_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcmf_em_supplier_loc') }}
),

final AS (
    SELECT
        batch_id,
        manufacturer_id,
        site_id,
        create_datetime,
        action_code
    FROM source_ff_xxcmf_em_supplier_loc
)

SELECT * FROM final
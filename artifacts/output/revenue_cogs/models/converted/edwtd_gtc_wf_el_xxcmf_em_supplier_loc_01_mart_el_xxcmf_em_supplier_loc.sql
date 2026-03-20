{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcmf_em_supplier_loc', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_XXCMF_EM_SUPPLIER_LOC',
        'target_table': 'EL_XXCMF_EM_SUPPLIER_LOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.695198+00:00'
    }
) }}

WITH 

source_el_xxcmf_em_supplier_loc AS (
    SELECT
        manufacturer_id,
        site_id,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'el_xxcmf_em_supplier_loc') }}
),

final AS (
    SELECT
        manufacturer_id,
        site_id,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_el_xxcmf_em_supplier_loc
)

SELECT * FROM final
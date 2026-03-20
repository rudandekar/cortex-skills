{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_apsp_bcbd_security', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_APSP_BCBD_SECURITY',
        'target_table': 'MT_APSP_BCBD_SECURITY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.744002+00:00'
    }
) }}

WITH 

source_mt_apsp_bcbd_security AS (
    SELECT
        dd_cec_id,
        security_component_descr
    FROM {{ source('raw', 'mt_apsp_bcbd_security') }}
),

final AS (
    SELECT
        dd_cec_id,
        security_component_descr
    FROM source_mt_apsp_bcbd_security
)

SELECT * FROM final
{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_wips_disti_cbn_mapping_intf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_DISTI_CBN_MAPPING_INTF',
        'target_table': 'ST_WIPS_DISTI_CBN_MAPPING_INTF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.575437+00:00'
    }
) }}

WITH 

source_ff_wips_disti_cbn_mapping_intf AS (
    SELECT
        id,
        cbn_cid,
        disti_profile_id
    FROM {{ source('raw', 'ff_wips_disti_cbn_mapping_intf') }}
),

final AS (
    SELECT
        id,
        cbn_cid,
        disti_profile_id
    FROM source_ff_wips_disti_cbn_mapping_intf
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tbl_disti_arch_mapping', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_TBL_DISTI_ARCH_MAPPING',
        'target_table': 'ST_TBL_DISTI_ARCH_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.727485+00:00'
    }
) }}

WITH 

source_ff_tbl_disti_arch_mapping AS (
    SELECT
        product_family,
        internal_sub_business_entity,
        internal_business_entity,
        mapping,
        disti_arch,
        disti_sub_arch,
        technologies
    FROM {{ source('raw', 'ff_tbl_disti_arch_mapping') }}
),

final AS (
    SELECT
        product_family,
        internal_sub_business_entity,
        internal_business_entity,
        mapping,
        disti_arch,
        disti_sub_arch,
        technologies
    FROM source_ff_tbl_disti_arch_mapping
)

SELECT * FROM final
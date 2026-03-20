{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_edwtd_replic_as_built_bom_prdt_lnk', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EDWTD_REPLIC_AS_BUILT_BOM_PRDT_LNK',
        'target_table': 'EDW_OBJECT_SWAP_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.179200+00:00'
    }
) }}

WITH 

source_edw_object_swap_info AS (
    SELECT
        job_group_id,
        rep_view,
        etl_view,
        temp_view,
        current_view,
        active_indicator,
        last_swap_date
    FROM {{ source('raw', 'edw_object_swap_info') }}
),

final AS (
    SELECT
        job_group_id,
        rep_view,
        etl_view,
        temp_view,
        current_view,
        active_indicator,
        last_swap_date
    FROM source_edw_object_swap_info
)

SELECT * FROM final
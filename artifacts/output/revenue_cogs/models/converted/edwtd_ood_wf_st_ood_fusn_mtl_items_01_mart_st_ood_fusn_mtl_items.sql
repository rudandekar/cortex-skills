{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ood_fusn_mtl_items', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_ST_OOD_FUSN_MTL_ITEMS',
        'target_table': 'ST_OOD_FUSN_MTL_ITEMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.977555+00:00'
    }
) }}

WITH 

source_ff_ood_fusn_mtl_items AS (
    SELECT
        segment1,
        segment2,
        segment3,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ood_fusn_mtl_items') }}
),

final AS (
    SELECT
        segment1,
        segment2,
        segment3,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code
    FROM source_ff_ood_fusn_mtl_items
)

SELECT * FROM final
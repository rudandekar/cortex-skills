{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cst_item_cost', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_ST_CST_ITEM_COST',
        'target_table': 'FF_CST_ITEM_COSTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.147468+00:00'
    }
) }}

WITH 

source_ff_cst_item_costs AS (
    SELECT
        inventory_item_id,
        item_cost,
        last_update_date,
        creation_date
    FROM {{ source('raw', 'ff_cst_item_costs') }}
),

source_cst_item_costs AS (
    SELECT
        inventory_item_id,
        item_cost,
        last_update_date,
        creation_date
    FROM {{ source('raw', 'cst_item_costs') }}
),

final AS (
    SELECT
        inventory_item_id,
        item_cost,
        last_update_date,
        creation_date
    FROM source_cst_item_costs
)

SELECT * FROM final
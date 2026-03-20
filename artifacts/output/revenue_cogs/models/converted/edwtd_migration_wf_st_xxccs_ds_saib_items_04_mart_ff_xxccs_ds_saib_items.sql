{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxccs_ds_saib_items', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_FF_XXCCS_DS_SAIB_ITEMS',
        'target_table': 'FF_XXCCS_DS_SAIB_ITEMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.586158+00:00'
    }
) }}

WITH 

source_ff_xxccs_ds_saib_items AS (
    SELECT
        inventory_item_id,
        business_entity_desc_top,
        sub_business_entity_desc_top
    FROM {{ source('raw', 'ff_xxccs_ds_saib_items') }}
),

source_xxccs_ds_saib_items AS (
    SELECT
        inventory_item_id,
        business_entity_desc_top,
        sub_business_entity_desc_top
    FROM {{ source('raw', 'xxccs_ds_saib_items') }}
),

final AS (
    SELECT
        inventory_item_id,
        business_entity_desc_top,
        sub_business_entity_desc_top
    FROM source_xxccs_ds_saib_items
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_kit_components_list', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_KIT_COMPONENTS_LIST',
        'target_table': 'FF_XXCFI_CB_KIT_COMPONENTS_LIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.563056+00:00'
    }
) }}

WITH 

source_xxcfi_cb_kit_components_list AS (
    SELECT
        kit_component_id,
        kit_component_name,
        custom_kit_id,
        custom_kit_name,
        quantity_value,
        standard_cost,
        value_percent,
        created_by,
        created_date,
        modified_by,
        modified_date
    FROM {{ source('raw', 'xxcfi_cb_kit_components_list') }}
),

transformed_ex_ff_xxcfi_cb_kit_components_list AS (
    SELECT
    kit_component_id,
    kit_component_name,
    custom_kit_id,
    custom_kit_name,
    quantity_value,
    standard_cost,
    value_percent,
    created_by,
    created_date,
    modified_by,
    modified_date,
    'BatchId' AS batch_id,
    'I' AS action_cd,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM source_xxcfi_cb_kit_components_list
),

final AS (
    SELECT
        kit_component_id,
        kit_component_name,
        custom_kit_id,
        custom_kit_name,
        quantity_value,
        standard_cost,
        value_percent,
        created_by,
        created_date,
        modified_by,
        modified_date,
        batch_id,
        action_cd,
        create_datetime
    FROM transformed_ex_ff_xxcfi_cb_kit_components_list
)

SELECT * FROM final
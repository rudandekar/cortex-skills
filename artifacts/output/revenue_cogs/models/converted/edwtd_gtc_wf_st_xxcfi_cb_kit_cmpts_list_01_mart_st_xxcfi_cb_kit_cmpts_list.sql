{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_kit_cmpts_list', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_KIT_CMPTS_LIST',
        'target_table': 'ST_XXCFI_CB_KIT_CMPTS_LIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.030487+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_kit_components_list AS (
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
    FROM {{ source('raw', 'ff_xxcfi_cb_kit_components_list') }}
),

final AS (
    SELECT
        batch_id,
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
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_kit_components_list
)

SELECT * FROM final
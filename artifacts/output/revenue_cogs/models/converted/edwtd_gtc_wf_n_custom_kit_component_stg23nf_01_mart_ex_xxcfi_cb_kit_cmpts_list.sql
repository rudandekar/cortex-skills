{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_custom_kit_component_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_CUSTOM_KIT_COMPONENT_STG23NF',
        'target_table': 'EX_XXCFI_CB_KIT_CMPTS_LIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.852221+00:00'
    }
) }}

WITH 

source_ex_xxcfi_cb_kit_cmpts_list AS (
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
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_kit_cmpts_list') }}
),

source_n_custom_kit_component AS (
    SELECT
        custom_item_key,
        component_item_key,
        component_value_pct_rt,
        component_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_custom_kit_component') }}
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
        action_code,
        exception_type
    FROM source_n_custom_kit_component
)

SELECT * FROM final
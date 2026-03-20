{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_item_classification', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_ITEM_CLASSIFICATION',
        'target_table': 'FF_XXCFI_CB_ITEM_CLASSIFICATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.401099+00:00'
    }
) }}

WITH 

source_xxcfi_cb_item_classification AS (
    SELECT
        item_classification_id,
        item_id,
        item_name,
        inventory_item_id,
        item_type,
        erp_specific,
        custom_specific,
        hts_code,
        start_date,
        end_date,
        country_group_id,
        country_group_code,
        rule_id,
        classification_type,
        tc_interface_flag,
        created_by,
        created_date,
        modified_by,
        modified_date,
        end_dated_flag,
        nextlinx_rule_id,
        hts_start_date,
        item_flag,
        rule_modify_flag,
        erp_specific_id,
        custom_specific_id
    FROM {{ source('raw', 'xxcfi_cb_item_classification') }}
),

transformed_exp_ff_cb_item_classification AS (
    SELECT
    batch_id,
    item_classification_id,
    item_id,
    item_name,
    inventory_item_id,
    item_type,
    erp_specific,
    custom_specific,
    hts_code,
    start_date,
    end_date,
    country_group_id,
    country_group_code,
    rule_id,
    classification_type,
    tc_interface_flag,
    created_by,
    created_date,
    modified_by,
    modified_date,
    end_dated_flag,
    nextlinx_rule_id,
    hts_start_date,
    item_flag,
    rule_modify_flag,
    erp_specific_id,
    custom_specific_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_item_classification
),

final AS (
    SELECT
        batch_id,
        item_classification_id,
        item_id,
        item_name,
        inventory_item_id,
        item_type,
        erp_specific,
        custom_specific,
        hts_code,
        start_date,
        end_date,
        country_group_id,
        country_group_code,
        rule_id,
        classification_type,
        tc_interface_flag,
        created_by,
        created_date,
        modified_by,
        modified_date,
        end_dated_flag,
        nextlinx_rule_id,
        hts_start_date,
        item_flag,
        rule_modify_flag,
        erp_specific_id,
        custom_specific_id,
        create_datetime,
        action_code
    FROM transformed_exp_ff_cb_item_classification
)

SELECT * FROM final
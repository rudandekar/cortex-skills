{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_item_classification', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_ITEM_CLASSIFICATION',
        'target_table': 'ST_XXCFI_CB_ITEM_CLASSFCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.364335+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_item_classification AS (
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
    FROM {{ source('raw', 'ff_xxcfi_cb_item_classification') }}
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
    FROM source_ff_xxcfi_cb_item_classification
)

SELECT * FROM final
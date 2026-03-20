{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_data_source', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_DATA_SOURCE',
        'target_table': 'FF_XXCFI_CB_DATA_SOURCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.188833+00:00'
    }
) }}

WITH 

source_xxcfi_cb_data_source AS (
    SELECT
        data_source_id,
        data_source,
        description,
        classification_type,
        sync_to_tc_flag,
        item_type,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date
    FROM {{ source('raw', 'xxcfi_cb_data_source') }}
),

transformed_exp_xxcfi_cb_data_source AS (
    SELECT
    data_source_id,
    data_source,
    description,
    classification_type,
    sync_to_tc_flag,
    item_type,
    start_date,
    end_date,
    created_by,
    created_date,
    modified_by,
    modified_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_data_source
),

final AS (
    SELECT
        batch_id,
        data_source_id,
        data_source,
        description,
        classification_type,
        sync_to_tc_flag,
        item_type,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code
    FROM transformed_exp_xxcfi_cb_data_source
)

SELECT * FROM final
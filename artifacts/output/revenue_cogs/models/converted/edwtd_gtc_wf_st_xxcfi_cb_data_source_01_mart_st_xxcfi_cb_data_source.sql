{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_data_source', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_DATA_SOURCE',
        'target_table': 'ST_XXCFI_CB_DATA_SOURCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.076729+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_data_source AS (
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
    FROM {{ source('raw', 'ff_xxcfi_cb_data_source') }}
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
    FROM source_ff_xxcfi_cb_data_source
)

SELECT * FROM final
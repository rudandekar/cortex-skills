{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_type_specific', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_TYPE_SPECIFIC',
        'target_table': 'FF_XXCFI_CB_TYPE_SPECIFIC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.505087+00:00'
    }
) }}

WITH 

source_xxcfi_cb_type_specific AS (
    SELECT
        classification_attributes_id,
        product_type_class_code,
        product_flag,
        description,
        specific,
        comments,
        attribute1,
        attribute2,
        attribute3,
        deactivated_by,
        deactivated_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        source,
        specific_id
    FROM {{ source('raw', 'xxcfi_cb_type_specific') }}
),

transformed_exp_ff_xxcfi_cb_type_specific AS (
    SELECT
    classification_attributes_id,
    product_type_class_code,
    product_flag,
    description,
    specific,
    comments,
    attribute1,
    attribute2,
    attribute3,
    deactivated_by,
    deactivated_date,
    created_by,
    created_date,
    modified_by,
    modified_date,
    source,
    specific_id,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_type_specific
),

final AS (
    SELECT
        batch_id,
        classification_attributes_id,
        product_type_class_code,
        product_flag,
        description,
        specific,
        comments,
        attribute1,
        attribute2,
        attribute3,
        deactivated_by,
        deactivated_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        source,
        specific_id,
        create_datetime,
        action_code
    FROM transformed_exp_ff_xxcfi_cb_type_specific
)

SELECT * FROM final
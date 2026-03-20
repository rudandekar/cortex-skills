{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_po_control_groups_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_PO_CONTROL_GROUPS_ALL',
        'target_table': 'FF_MF_PO_CONTROL_GROUPS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.131532+00:00'
    }
) }}

WITH 

source_mf_po_control_groups_all AS (
    SELECT
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category,
        control_group_id,
        control_group_name,
        created_by,
        creation_date,
        description,
        enabled_flag,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        org_id,
        program_application_id,
        program_id,
        program_update_date,
        request_id
    FROM {{ source('raw', 'mf_po_control_groups_all') }}
),

transformed_exp_mf_po_control_groups_all AS (
    SELECT
    attribute1,
    attribute10,
    attribute11,
    attribute14,
    attribute2,
    attribute8,
    control_group_id,
    control_group_name,
    created_by,
    creation_date,
    description,
    enabled_flag,
    ges_update_date,
    global_name,
    last_updated_by,
    last_update_date,
    last_update_login,
    org_id,
    program_application_id,
    program_id,
    program_update_date,
    request_id,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_mf_po_control_groups_all
),

final AS (
    SELECT
        batch_id,
        attribute1,
        attribute10,
        attribute11,
        attribute14,
        attribute2,
        attribute8,
        control_group_id,
        control_group_name,
        created_by,
        creation_date,
        description,
        enabled_flag,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        org_id,
        program_application_id,
        program_id,
        program_update_date,
        request_id,
        create_datetime,
        action_code
    FROM transformed_exp_mf_po_control_groups_all
)

SELECT * FROM final
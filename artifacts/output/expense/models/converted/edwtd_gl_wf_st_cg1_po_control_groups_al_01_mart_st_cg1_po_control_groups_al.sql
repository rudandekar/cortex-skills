{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_po_control_groups_al', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_PO_CONTROL_GROUPS_AL',
        'target_table': 'ST_CG1_PO_CONTROL_GROUPS_AL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.718472+00:00'
    }
) }}

WITH 

source_ff_cg1_po_control_groups_al AS (
    SELECT
        control_group_id,
        control_group_name,
        last_update_date,
        last_updated_by,
        last_update_login,
        creation_date,
        created_by,
        enabled_flag,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        description,
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
        org_id,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_cg1_po_control_groups_al') }}
),

final AS (
    SELECT
        control_group_id,
        control_group_name,
        last_update_date,
        last_updated_by,
        last_update_login,
        creation_date,
        created_by,
        enabled_flag,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        description,
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
        org_id,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM source_ff_cg1_po_control_groups_al
)

SELECT * FROM final
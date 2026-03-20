{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_po_control_groups_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_PO_CONTROL_GROUPS_ALL',
        'target_table': 'EL_PO_CONTROL_GROUPS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.635252+00:00'
    }
) }}

WITH 

source_st_mf_po_control_groups_all AS (
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
    FROM {{ source('raw', 'st_mf_po_control_groups_all') }}
),

final AS (
    SELECT
        attribute1,
        attribute10,
        attribute11,
        attribute14,
        attribute2,
        attribute8,
        control_group_id,
        control_group_name,
        creation_date,
        enabled_flag,
        ges_update_date,
        global_name,
        last_update_date,
        org_id
    FROM source_st_mf_po_control_groups_all
)

SELECT * FROM final
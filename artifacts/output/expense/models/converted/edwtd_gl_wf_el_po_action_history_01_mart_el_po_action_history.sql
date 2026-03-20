{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_po_action_history', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_PO_ACTION_HISTORY',
        'target_table': 'EL_PO_ACTION_HISTORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.861384+00:00'
    }
) }}

WITH 

source_st_mf_po_action_history AS (
    SELECT
        batch_id,
        action_code_value,
        action_date,
        approval_path_id,
        created_by,
        creation_date,
        employee_id,
        last_update_date,
        last_updated_by,
        note,
        object_id,
        object_revision_num,
        object_sub_type_code,
        object_type_code,
        offline_code,
        sequence_num,
        ges_pk_id,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_po_action_history') }}
),

final AS (
    SELECT
        action_code_value,
        action_date,
        creation_date,
        employee_id,
        last_update_date,
        note,
        object_id,
        object_revision_num,
        object_sub_type_code,
        object_type_code,
        offline_code,
        ges_pk_id,
        sequence_num,
        ges_update_date,
        global_name
    FROM source_st_mf_po_action_history
)

SELECT * FROM final
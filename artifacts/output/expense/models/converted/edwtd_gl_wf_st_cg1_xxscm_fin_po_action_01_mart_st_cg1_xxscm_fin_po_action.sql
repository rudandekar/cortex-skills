{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxscm_fin_po_action', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXSCM_FIN_PO_ACTION',
        'target_table': 'ST_CG1_XXSCM_FIN_PO_ACTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.686777+00:00'
    }
) }}

WITH 

source_ff_cg1_xxscm_fin_po_action AS (
    SELECT
        poah_id,
        object_id,
        object_type_code,
        object_sub_type_code,
        sequence_num,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        action_code,
        action_date,
        employee_id,
        approval_path_id,
        note,
        object_revision_num,
        offline_code,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        program_date,
        approval_group_id,
        ges_update_date,
        global_name,
        batch_id,
        created_dt,
        action_code_td
    FROM {{ source('raw', 'ff_cg1_xxscm_fin_po_action') }}
),

final AS (
    SELECT
        poah_id,
        object_id,
        object_type_code,
        object_sub_type_code,
        sequence_num,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        action_code_value,
        action_date,
        employee_id,
        approval_path_id,
        note,
        object_revision_num,
        offline_code,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        program_date,
        approval_group_id,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM source_ff_cg1_xxscm_fin_po_action
)

SELECT * FROM final
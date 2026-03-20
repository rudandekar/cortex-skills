{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_iam_edwtd_usr_rol_geo_ent', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_ST_IAM_EDWTD_USR_ROL_GEO_ENT',
        'target_table': 'ST_IAM_EDWTD_USR_ROL_GEO_ENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.763320+00:00'
    }
) }}

WITH 

source_ff_iam_edwtd_usr_rol_geo_ent AS (
    SELECT
        batch_id,
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        erp_segment1,
        assignment_type,
        data_res_expt_flag,
        excl_restr_flag,
        restriction_flag,
        proxy_flag,
        grantor_universal_id,
        grantor_cec_id,
        grantor_cco_id,
        status,
        tr_flag,
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_iam_edwtd_usr_rol_geo_ent') }}
),

final AS (
    SELECT
        batch_id,
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        erp_segment1,
        assignment_type,
        data_res_expt_flag,
        excl_restr_flag,
        restriction_flag,
        proxy_flag,
        grantor_universal_id,
        grantor_cec_id,
        grantor_cco_id,
        status,
        tr_flag,
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        create_datetime,
        action_code
    FROM source_ff_iam_edwtd_usr_rol_geo_ent
)

SELECT * FROM final
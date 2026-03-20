{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_usr_rol_geo_ent', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_USR_ROL_GEO_ENT',
        'target_table': 'FF_IAM_EDWTD_USR_ROL_GEO_ENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.788594+00:00'
    }
) }}

WITH 

source_iam_edwtd_usr_rol_geo_ent_vw AS (
    SELECT
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
        update_date
    FROM {{ source('raw', 'iam_edwtd_usr_rol_geo_ent_vw') }}
),

transformed_exptrans AS (
    SELECT
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
    'BatchId' AS batch_id,
    CURRENT_DATE() AS create_datetime,
    'I' AS action_code
    FROM source_iam_edwtd_usr_rol_geo_ent_vw
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
    FROM transformed_exptrans
)

SELECT * FROM final
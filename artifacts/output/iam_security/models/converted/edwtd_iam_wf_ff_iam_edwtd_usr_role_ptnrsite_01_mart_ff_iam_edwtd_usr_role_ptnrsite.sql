{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_usr_role_ptnrsite', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_USR_ROLE_PTNRSITE',
        'target_table': 'FF_IAM_EDWTD_USR_ROLE_PTNRSITE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.742817+00:00'
    }
) }}

WITH 

source_iam_edwtd_usr_role_ptnrsite_vw AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        site_target_id,
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
        erp_segment1,
        is_user_sel_par,
        pdb_site_id,
        created_by,
        create_date,
        updated_by,
        update_date
    FROM {{ source('raw', 'iam_edwtd_usr_role_ptnrsite_vw') }}
),

transformed_exptrans AS (
    SELECT
    iam_user_id,
    role_id,
    role_name,
    tool_id,
    data_struct_type,
    lvl_number,
    site_target_id,
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
    erp_segment1,
    is_user_sel_par,
    pdb_site_id,
    created_by,
    create_date,
    updated_by,
    update_date,
    'I' AS action_code,
    'BatchId' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_edwtd_usr_role_ptnrsite_vw
),

final AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        site_target_id,
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
        erp_segment1,
        is_user_sel_par,
        pdb_site_id,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM transformed_exptrans
)

SELECT * FROM final
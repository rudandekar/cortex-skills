{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_bv_fin_sls_theater_hier', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_BV_FIN_SLS_THEATER_HIER',
        'target_table': 'EX_IAM_EDWTD_USR_ROLE_SF_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.729961+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_role_sf_hier AS (
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
        batch_id,
        action_cd,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_sf_hier') }}
),

final AS (
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
        batch_id,
        action_cd,
        create_datetime,
        exception_type
    FROM source_st_iam_edwtd_usr_role_sf_hier
)

SELECT * FROM final
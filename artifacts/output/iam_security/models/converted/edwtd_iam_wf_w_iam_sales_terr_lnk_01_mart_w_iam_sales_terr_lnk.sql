{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_sales_terr_lnk', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_SALES_TERR_LNK',
        'target_table': 'W_IAM_SALES_TERR_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.780485+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_role_shr_hier AS (
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
        territory_type_code,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_shr_hier') }}
),

source_st_iam_edwtd_usr_role_shr_hier AS (
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
        action_code,
        batch_id,
        create_datetime,
        territory_type_code
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_shr_hier') }}
),

final AS (
    SELECT
        sales_territory_key,
        iam_application_key,
        iam_user_key,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        iam_level_num_int,
        dd_bk_sales_hier_type_cd,
        dd_bk_sales_terr_name,
        source_deleted_flg,
        dd_cec_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dd_sales_territory_descr,
        start_tv_dt,
        end_tv_dt,
        bk_iam_role_name,
        bk_sales_terr_assgnmnt_type_cd,
        action_code,
        dml_type
    FROM source_st_iam_edwtd_usr_role_shr_hier
)

SELECT * FROM final
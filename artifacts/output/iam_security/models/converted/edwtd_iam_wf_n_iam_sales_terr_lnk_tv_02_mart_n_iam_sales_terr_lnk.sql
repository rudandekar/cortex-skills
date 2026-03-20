{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_sales_terr_lnk_tv', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_SALES_TERR_LNK_TV',
        'target_table': 'N_IAM_SALES_TERR_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.758967+00:00'
    }
) }}

WITH 

source_w_iam_sales_terr_lnk AS (
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
    FROM {{ source('raw', 'w_iam_sales_terr_lnk') }}
),

source_n_iam_sales_terr_lnk_tv AS (
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
        bk_sales_terr_assgnmnt_type_cd
    FROM {{ source('raw', 'n_iam_sales_terr_lnk_tv') }}
),

final AS (
    SELECT
        bk_iam_role_name,
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
        bk_sales_terr_assgnmnt_type_cd
    FROM source_n_iam_sales_terr_lnk_tv
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_busmbr_iam_sales_terr_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_BUSMBR_IAM_SALES_TERR_LINK',
        'target_table': 'MT_BUSMBR_IAM_SALES_TERR_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.781870+00:00'
    }
) }}

WITH 

source_mt_iam_salesbo_sls_terr_link AS (
    SELECT
        application_name,
        iam_user_key,
        iam_application_key,
        bk_cpr_universal_id_int,
        internal_role,
        external_role,
        ru_cisco_worker_party_key,
        cco_id,
        source_deleted_flg,
        dd_bk_worker_type_cd,
        dd_cec_id,
        ru_external_flg,
        ru_internal_flg,
        ru_iam_dummy_user_name,
        ru_external_user_email_addr,
        bk_iam_role_name,
        sales_territory_key,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        iam_level_num_int,
        dd_bk_sales_hier_type_cd,
        dd_bk_sales_terr_name,
        dd_sales_territory_descr,
        fiscal_year_month_int,
        dummy_user_role,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        bk_sales_terr_assgnmnt_type_cd
    FROM {{ source('raw', 'mt_iam_salesbo_sls_terr_link') }}
),

final AS (
    SELECT
        application_name,
        iam_user_key,
        iam_application_key,
        bk_cpr_universal_id_int,
        internal_role,
        external_role,
        ru_cisco_worker_party_key,
        cco_id,
        source_deleted_flg,
        dd_bk_worker_type_cd,
        dd_cec_id,
        ru_external_flg,
        ru_internal_flg,
        ru_iam_dummy_user_name,
        ru_external_user_email_addr,
        bk_iam_role_name,
        sales_territory_key,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        iam_level_num_int,
        dd_bk_sales_hier_type_cd,
        dd_bk_sales_terr_name,
        dd_sales_territory_descr,
        fiscal_year_month_int,
        dummy_user_role,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        bk_sales_terr_assgnmnt_type_cd,
        sales_rep_number,
        am_flg,
        fiscal_month_name,
        sales_terr_assgn_type_name,
        dv_am_explosion_view_flg
    FROM source_mt_iam_salesbo_sls_terr_link
)

SELECT * FROM final
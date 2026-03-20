{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_apsp_bcbd_erp_sls_rep_lnk', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_APSP_BCBD_ERP_SLS_REP_LNK',
        'target_table': 'WI_USER_VISIBILITY_LOAD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.773084+00:00'
    }
) }}

WITH 

source_st_ca_fin_visibility_sec_load1 AS (
    SELECT
        user_id,
        min_level,
        biz_role_name,
        subrole_name,
        territory_code,
        role_name,
        fiscal_year,
        load_type,
        load_flag,
        sales_territory_code
    FROM {{ source('raw', 'st_ca_fin_visibility_sec_load1') }}
),

source_w_apsp_bcbd_erp_sls_rep_lnk AS (
    SELECT
        bcbd_user_key,
        bk_iam_role_name,
        iam_application_key,
        bk_sales_rep_num,
        bk_fiscal_year_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_bcbd_erp_sls_rep_lnk') }}
),

final AS (
    SELECT
        user_id,
        min_level,
        biz_role_name,
        subrole_name,
        territory_code,
        role_name,
        fiscal_year,
        load_flag,
        bk_sales_terr_assgnmnt_type_cd
    FROM source_w_apsp_bcbd_erp_sls_rep_lnk
)

SELECT * FROM final
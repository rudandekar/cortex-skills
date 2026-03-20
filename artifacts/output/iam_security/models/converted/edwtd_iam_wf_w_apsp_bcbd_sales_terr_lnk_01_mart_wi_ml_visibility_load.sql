{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_apsp_bcbd_sales_terr_lnk', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_APSP_BCBD_SALES_TERR_LNK',
        'target_table': 'WI_ML_VISIBILITY_LOAD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.719532+00:00'
    }
) }}

WITH 

source_st_ca_fin_visibility_sec_load AS (
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
    FROM {{ source('raw', 'st_ca_fin_visibility_sec_load') }}
),

source_w_apsp_bcbd_sales_terr_lnk AS (
    SELECT
        bcbd_user_key,
        bk_iam_role_name,
        iam_application_key,
        sales_territory_key,
        bk_fiscal_year_num,
        standard_or_exception_flg,
        iam_level_num_int,
        bi_cafe_role_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_bcbd_sales_terr_lnk') }}
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
        bk_sales_terr_assgnmnt_type_cd
    FROM source_w_apsp_bcbd_sales_terr_lnk
)

SELECT * FROM final
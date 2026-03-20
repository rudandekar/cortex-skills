{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_apsp_bcbd_visibility', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_APSP_BCBD_VISIBILITY',
        'target_table': 'WI_SALES_TERR_LNK_LVL0',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.750091+00:00'
    }
) }}

WITH 

source_wi_sales_terr_lnk_lvl0 AS (
    SELECT
        iam_user_key,
        bk_iam_role_name,
        iam_application_key,
        sales_territory_key,
        standard_or_exception_flg,
        iam_level_num_int,
        bicafe_role_name,
        bk_fiscal_year_number_int,
        territory_code,
        bk_sales_terr_assgnmnt_type_cd
    FROM {{ source('raw', 'wi_sales_terr_lnk_lvl0') }}
),

final AS (
    SELECT
        iam_user_key,
        bk_iam_role_name,
        iam_application_key,
        sales_territory_key,
        standard_or_exception_flg,
        iam_level_num_int,
        bicafe_role_name,
        bk_fiscal_year_number_int,
        territory_code,
        bk_sales_terr_assgnmnt_type_cd
    FROM source_wi_sales_terr_lnk_lvl0
)

SELECT * FROM final
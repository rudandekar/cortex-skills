{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_opex_alloc_by_prdt_and_geo', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_OPEX_ALLOC_BY_PRDT_AND_GEO',
        'target_table': 'W_OPEX_ALLOC_BY_PRDT_AND_GEO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.171843+00:00'
    }
) }}

WITH 

source_w_opex_alloc_by_prdt_and_geo AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_department_cd,
        bk_financial_account_cd,
        sales_territory_key,
        bk_cntrbtn_mrgn_category_cd,
        product_key,
        bk_company_cd,
        bk_opex_mapping_type_name,
        bk_financial_location_cd,
        bk_subaccount_cd,
        bk_cb_mrgn_rollup_cat_cd,
        opex_end_fiscal_year_num_int,
        opex_end_fiscal_month_num_int,
        opex_strt_fiscal_year_num_int,
        opex_strt_fiscal_month_num_int,
        dv_bk_fiscal_year_month_int,
        incremental_flg,
        opex_allocation_pct,
        bk_project_code,
        bk_subaccount_locality_int,
        bk_fin_acct_locality_int,
        bk_project_locality_int,
        bk_product_family_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_opex_alloc_by_prdt_and_geo') }}
),

final AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_department_cd,
        bk_financial_account_cd,
        sales_territory_key,
        bk_cntrbtn_mrgn_category_cd,
        product_key,
        bk_company_cd,
        bk_opex_mapping_type_name,
        bk_financial_location_cd,
        bk_subaccount_cd,
        bk_cb_mrgn_rollup_cat_cd,
        opex_end_fiscal_year_num_int,
        opex_end_fiscal_month_num_int,
        opex_strt_fiscal_year_num_int,
        opex_strt_fiscal_month_num_int,
        dv_bk_fiscal_year_month_int,
        incremental_flg,
        opex_allocation_pct,
        bk_project_code,
        bk_subaccount_locality_int,
        bk_fin_acct_locality_int,
        bk_project_locality_int,
        bk_product_family_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        technology_group_shared_flg,
        action_code,
        dml_type
    FROM source_w_opex_alloc_by_prdt_and_geo
)

SELECT * FROM final
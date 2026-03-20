{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_eis_department', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_MT_EIS_DEPARTMENT',
        'target_table': 'MT_EIS_DEPARTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.647255+00:00'
    }
) }}

WITH 

source_n_financial_department AS (
    SELECT
        bk_company_code,
        bk_department_code,
        bk_budget_currency_code,
        bk_business_unit_id,
        bk_financial_location_code,
        bk_financial_market_lob_id,
        bk_financial_sales_region_id,
        bk_financial_tax_category_id,
        bk_functional_unit_id,
        cfp_manager_party_key,
        controller_party_key,
        estimated_budget_amount,
        estimated_head_count,
        fin_dept_usage_description,
        financial_analyst_party_key,
        financial_dept_enabled_flag,
        financial_dept_end_date,
        financial_dept_name,
        financial_dept_start_date,
        iso_country_code,
        management_hier_node_nm_key,
        pl_hierarchy_node_key,
        reclass_role,
        requisition_threshold_amount,
        ru_bk_reclass_company_code,
        ru_bk_reclass_department_code,
        sk_dept_no_num,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_financial_department') }}
),

final AS (
    SELECT
        bk_company_cd,
        department_id,
        sk_theater_id_int,
        department_short_id,
        department_descr,
        department_short_descr,
        financial_market_lob_descr,
        fin_company_name,
        l15_node_id_int,
        l15_descr,
        l14_node_id_int,
        l14_descr,
        l13_node_id_int,
        l13_descr,
        l12_node_id_int,
        l12_descr,
        l11_node_id_int,
        l11_descr,
        l10_node_id_int,
        l10_descr,
        l9_node_id_int,
        l9_descr,
        l8_node_id_int,
        l8_descr,
        l7_node_id_int,
        l7_descr,
        l6_node_id_int,
        l6_descr,
        l5_node_id_int,
        l5_descr,
        l4_node_id_int,
        l4_descr,
        l3_node_id_int,
        l3_descr,
        l2_node_id_int,
        l2_descr,
        l1_node_id_int,
        l1_descr,
        hierarchy_type,
        edw_update_user,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_n_financial_department
)

SELECT * FROM final
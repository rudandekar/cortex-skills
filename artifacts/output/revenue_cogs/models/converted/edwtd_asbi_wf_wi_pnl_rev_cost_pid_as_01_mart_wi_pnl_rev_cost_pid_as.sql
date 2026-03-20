{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_rev_cost_pid_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_REV_COST_PID_AS',
        'target_table': 'WI_PNL_REV_COST_PID_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.735858+00:00'
    }
) }}

WITH 

source_wi_pnl_rev_cost_pid_as AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        fiscal_year_number,
        fiscal_month_name,
        rev_measure_trans_type_cd,
        sales_territory_key,
        bk_allocated_servc_group_id,
        dv_data_source_name,
        dv_data_set_cd,
        pnl_line_item_name,
        record_type,
        software_service_category,
        product_family_id,
        product_category_cd,
        software_stack_cd,
        monetization_type_cd,
        business_unit_id,
        technology_group_id,
        product_classification,
        tms_level2,
        int_sub_be,
        int_sub_be_descr,
        int_be,
        int_be_descr,
        business_group_descr,
        ext_sub_be,
        ext_sub_be_descr,
        ext_be,
        ext_be_descr,
        tss_rev,
        software_usage_type_cd,
        l1_svc_offer_hierarchy_name,
        l2_svc_offer_hierarchy_name,
        l3_svc_offer_hierarchy_name,
        divestiture_flg
    FROM {{ source('raw', 'wi_pnl_rev_cost_pid_as') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        fiscal_year_number,
        fiscal_month_name,
        rev_measure_trans_type_cd,
        sales_territory_key,
        bk_allocated_servc_group_id,
        dv_data_source_name,
        dv_data_set_cd,
        pnl_line_item_name,
        record_type,
        software_service_category,
        product_family_id,
        product_category_cd,
        software_stack_cd,
        monetization_type_cd,
        business_unit_id,
        technology_group_id,
        product_classification,
        tms_level2,
        int_sub_be,
        int_sub_be_descr,
        int_be,
        int_be_descr,
        business_group_descr,
        ext_sub_be,
        ext_sub_be_descr,
        ext_be,
        ext_be_descr,
        tss_rev,
        software_usage_type_cd,
        l1_svc_offer_hierarchy_name,
        l2_svc_offer_hierarchy_name,
        l3_svc_offer_hierarchy_name,
        divestiture_flg
    FROM source_wi_pnl_rev_cost_pid_as
)

SELECT * FROM final
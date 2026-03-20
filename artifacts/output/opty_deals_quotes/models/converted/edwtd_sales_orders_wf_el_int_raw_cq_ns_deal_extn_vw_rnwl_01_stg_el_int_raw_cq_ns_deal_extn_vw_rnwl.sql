{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_raw_cq_ns_deal_extn_vw_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RAW_CQ_NS_DEAL_EXTN_VW_RNWL',
        'target_table': 'EL_INT_RAW_CQ_NS_DEAL_EXTN_VW_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.942270+00:00'
    }
) }}

WITH 

source_el_int_raw_cq_ns_deal_extn_vw_rnwl AS (
    SELECT
        deal_object_id,
        approval_route,
        approval_route_name,
        product_comp,
        tss_core_comp,
        as_transaction_comp,
        as_subscription_comp,
        as_fixed_comp,
        ros_comp,
        product_component,
        service_component,
        update_datetime,
        get_well_plan_descr,
        flag_spvtg,
        dsa_regstr_date,
        dd_engage_date,
        ns_summary_descr,
        exe_summary_internal_descr,
        is_sa_deal,
        create_datetime
    FROM {{ source('raw', 'el_int_raw_cq_ns_deal_extn_vw_rnwl') }}
),

final AS (
    SELECT
        deal_object_id,
        approval_route,
        approval_route_name,
        product_comp,
        tss_core_comp,
        as_transaction_comp,
        as_subscription_comp,
        as_fixed_comp,
        ros_comp,
        product_component,
        service_component,
        update_datetime,
        get_well_plan_descr,
        flag_spvtg,
        dsa_regstr_date,
        dd_engage_date,
        ns_summary_descr,
        exe_summary_internal_descr,
        is_sa_deal,
        create_datetime
    FROM source_el_int_raw_cq_ns_deal_extn_vw_rnwl
)

SELECT * FROM final
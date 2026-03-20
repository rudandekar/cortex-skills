{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_cq_ns_dl_extn_vw_f', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_CQ_NS_DL_EXTN_VW_F',
        'target_table': 'ST_INT_RAW_CQ_NS_DL_EXTN_VW_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.972818+00:00'
    }
) }}

WITH 

source_ff_st_int_raw_cq_ns_deal_extn_vw_kafka AS (
    SELECT
        parent_id,
        approval_route,
        approval_route_name,
        tss_core_comp,
        as_transaction_comp,
        as_subscription_comp,
        ros_comp,
        as_fixed_comp,
        get_well_plan_descr,
        dsa_regstr_date,
        dd_engage_date,
        ns_summary_descr,
        exe_summary_internal_descr,
        is_sa_deal,
        product_comp,
        service_component,
        product_component,
        action_code,
        create_datetime,
        created_by,
        created_on,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'ff_st_int_raw_cq_ns_deal_extn_vw_kafka') }}
),

final AS (
    SELECT
        parent_id,
        approval_route,
        approval_route_name,
        tss_core_comp,
        as_transaction_comp,
        as_subscription_comp,
        ros_comp,
        as_fixed_comp,
        get_well_plan_descr,
        dsa_regstr_date,
        dd_engage_date,
        ns_summary_descr,
        exe_summary_internal_descr,
        is_sa_deal,
        product_comp,
        service_component,
        product_component,
        action_code,
        create_datetime,
        created_by,
        created_on,
        updated_by,
        updated_on,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM source_ff_st_int_raw_cq_ns_deal_extn_vw_kafka
)

SELECT * FROM final
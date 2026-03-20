{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_rw_cq_ns_dl_etn_vw_rnwl_kafka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RW_CQ_NS_DL_ETN_VW_RNWL_KAFKA',
        'target_table': 'EL_INT_RW_CQ_NS_DL_ETN_VW_RNWL_KFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.874744+00:00'
    }
) }}

WITH 

source_el_int_rw_cq_ns_dl_etn_vw_rnwl_kfka AS (
    SELECT
        batch_id,
        deal_object_id,
        approval_route,
        approval_route_name,
        product_comp,
        tss_core_comp,
        as_transaction_comp,
        as_subscription_comp,
        as_fixed_comp,
        ros_comp,
        service_component,
        get_well_plan_descr,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_rw_cq_ns_dl_etn_vw_rnwl_kfka') }}
),

final AS (
    SELECT
        batch_id,
        deal_object_id,
        approval_route,
        approval_route_name,
        product_comp,
        tss_core_comp,
        as_transaction_comp,
        as_subscription_comp,
        as_fixed_comp,
        ros_comp,
        service_component,
        get_well_plan_descr,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        edw_update_dtm,
        edw_update_user
    FROM source_el_int_rw_cq_ns_dl_etn_vw_rnwl_kfka
)

SELECT * FROM final
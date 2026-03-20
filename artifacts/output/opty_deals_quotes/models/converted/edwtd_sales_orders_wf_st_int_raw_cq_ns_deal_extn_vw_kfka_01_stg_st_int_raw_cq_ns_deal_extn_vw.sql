{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_raw_cq_ns_deal_extn_vw_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RAW_CQ_NS_DEAL_EXTN_VW_KFKA',
        'target_table': 'ST_INT_RAW_CQ_NS_DEAL_EXTN_VW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.875056+00:00'
    }
) }}

WITH 

source_el_int_rw_cq_ns_dl_etn_vw_kfka AS (
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
        product_component,
        service_component,
        created_by,
        created_on,
        updated_by,
        updated_on,
        get_well_plan_descr,
        flag_spvtg,
        dsa_regstr_date,
        dd_engage_date,
        ns_summary_descr,
        exe_summary_internal_descr,
        is_sa_deal,
        create_datetime,
        action_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_rw_cq_ns_dl_etn_vw_kfka') }}
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
        product_component,
        service_component,
        created_by,
        created_on,
        updated_by,
        updated_on,
        get_well_plan_descr,
        flag_spvtg,
        dsa_regstr_date,
        dd_engage_date,
        ns_summary_descr,
        exe_summary_internal_descr,
        is_sa_deal,
        create_datetime,
        action_code
    FROM source_el_int_rw_cq_ns_dl_etn_vw_kfka
)

SELECT * FROM final
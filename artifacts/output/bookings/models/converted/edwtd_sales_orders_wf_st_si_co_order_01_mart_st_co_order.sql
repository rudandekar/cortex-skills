{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_co_order', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_SI_CO_ORDER',
        'target_table': 'ST_CO_ORDER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.979545+00:00'
    }
) }}

WITH 

source_st_si_co_order AS (
    SELECT
        order_id,
        order_name,
        order_source,
        order_status,
        order_version,
        org_id,
        partial_order_flag,
        price_list_id,
        purchase_order_number,
        quote_id,
        submitted_by,
        submitted_by_proxy,
        updated_by,
        active,
        application_flow,
        web_order_id,
        application_sub_flow,
        created_by,
        created_by_proxy,
        currency_code,
        deal_id,
        flex_service_start_date,
        fsd_validated,
        hold_flag,
        intended_use,
        xaas_order_flag,
        flow_type,
        source_dml_type,
        updated_on,
        created_on,
        submitted_on,
        kafka_create_datetime,
        kafka_offset,
        kafka_partition,
        total_effective_discount_pct
    FROM {{ source('raw', 'st_si_co_order') }}
),

final AS (
    SELECT
        order_id,
        order_name,
        order_source,
        order_status,
        order_version,
        org_id,
        partial_order_flag,
        price_list_id,
        purchase_order_number,
        quote_id,
        submitted_by,
        submitted_by_proxy,
        submitted_on,
        updated_by,
        updated_on,
        active,
        application_flow,
        web_order_id,
        application_sub_flow,
        created_by,
        created_by_proxy,
        created_on,
        currency_code,
        deal_id,
        flex_service_start_date,
        fsd_validated,
        hold_flag,
        intended_use,
        xaas_order_flag,
        flow_type,
        kafka_create_datetime,
        total_effective_discount_pct
    FROM source_st_si_co_order
)

SELECT * FROM final
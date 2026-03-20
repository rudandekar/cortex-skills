{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ssc_rma_volume', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SSC_RMA_VOLUME',
        'target_table': 'WI_SSC_RMA_VOLUME',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.957636+00:00'
    }
) }}

WITH 

source_wi_ssc_rma_volume AS (
    SELECT
        sr_number,
        fiscal_month_id,
        order_number,
        product_family,
        fiscal_ship_quarter,
        extended_standard_cost,
        site_country,
        site_theater,
        order_sa_number,
        order_sa_service_line,
        product_part_number,
        contracted_service_group,
        quantity_shipped,
        requested_service_group,
        extended_list_price,
        ship_fiscal_month,
        sr_type,
        lookup_theater,
        created_date,
        cust_mkt_segment,
        site_cust_name,
        ent_product_family,
        failure_code,
        incident_status,
        closed_date,
        ship_date,
        warranty_parts_pct,
        labor_line_flag
    FROM {{ source('raw', 'wi_ssc_rma_volume') }}
),

source_wi_ssc_rma_interim AS (
    SELECT
        sr_number,
        fiscal_month_id,
        order_number,
        product_family,
        fiscal_ship_quarter,
        extended_standard_cost,
        site_country,
        site_theater,
        order_sa_number,
        order_sa_service_line,
        product_part_number,
        contracted_service_group,
        quantity_shipped,
        requested_service_group,
        extended_list_price,
        ship_fiscal_month,
        sr_type,
        lookup_theater,
        created_date,
        cust_mkt_segment,
        site_cust_name,
        ent_product_family,
        failure_code,
        incident_status,
        closed_date,
        ship_date,
        labor_line_flag
    FROM {{ source('raw', 'wi_ssc_rma_interim') }}
),

final AS (
    SELECT
        sr_number,
        fiscal_month_id,
        order_number,
        product_family,
        fiscal_ship_quarter,
        extended_standard_cost,
        site_country,
        site_theater,
        order_sa_number,
        order_sa_service_line,
        product_part_number,
        contracted_service_group,
        quantity_shipped,
        requested_service_group,
        extended_list_price,
        ship_fiscal_month,
        sr_type,
        lookup_theater,
        created_date,
        cust_mkt_segment,
        site_cust_name,
        ent_product_family,
        failure_code,
        incident_status,
        closed_date,
        ship_date,
        warranty_parts_pct,
        labor_line_flag
    FROM source_wi_ssc_rma_interim
)

SELECT * FROM final
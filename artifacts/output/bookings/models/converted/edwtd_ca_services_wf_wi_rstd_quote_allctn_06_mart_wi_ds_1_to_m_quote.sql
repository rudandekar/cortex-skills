{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_quote_allctn', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_QUOTE_ALLCTN',
        'target_table': 'WI_DS_1_TO_M_QUOTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.249972+00:00'
    }
) }}

WITH 

source_wi_quote_extract_merge AS (
    SELECT
        quote_number,
        order_number,
        quote_status_cd,
        contract_num,
        target_contract_num,
        coverage_start_dt,
        coverage_end_dt,
        service_type,
        cle_id_renewed,
        sk_instance_id_int,
        product_id,
        serial_number,
        shipped_date,
        quote_line_status_cd,
        total_list_price,
        maintenance_list_price,
        list_quote_price,
        order_quote_key,
        svcsku_key,
        system_source_quotes,
        fiscal_year,
        quarter_ref,
        fiscal_period_id,
        quote_type
    FROM {{ source('raw', 'wi_quote_extract_merge') }}
),

source_wi_rstd_quote_allctn AS (
    SELECT
        order_number,
        quote_number,
        product_subgroup,
        product_id,
        product_key,
        quote_net_pid,
        order_psg_total_quote_price,
        quote_alloc_pct
    FROM {{ source('raw', 'wi_rstd_quote_allctn') }}
),

source_wi_rstd_quote_allctn_1_to_m AS (
    SELECT
        serviceordernumber_curr,
        quote_number,
        product_subgroup,
        product_id,
        product_key,
        quote_net_pid,
        order_psg_total_quote_price,
        quote_alloc_pct
    FROM {{ source('raw', 'wi_rstd_quote_allctn_1_to_m') }}
),

source_wi_svc_line_pct_updated AS (
    SELECT
        serviceordernumber_pre,
        serviceordernumber_curr,
        quote_number,
        psg,
        product_id,
        product_key,
        quote_net_pid
    FROM {{ source('raw', 'wi_svc_line_pct_updated') }}
),

source_wi_ds_1_to_m_quote AS (
    SELECT
        quote_number,
        serviceordernumber
    FROM {{ source('raw', 'wi_ds_1_to_m_quote') }}
),

source_wi_svc_pct AS (
    SELECT
        serviceordernumber,
        quote_number,
        quote_order_booking,
        total_bookings,
        bookings_pct,
        cum_pct_at_order,
        min_range,
        max_range
    FROM {{ source('raw', 'wi_svc_pct') }}
),

source_wi_ds_quote AS (
    SELECT
        order_number,
        quote_number,
        product_subgroup,
        product_id,
        product_key,
        quote_net_pid,
        order_psg_total_quote_price,
        quote_alloc_pct
    FROM {{ source('raw', 'wi_ds_quote') }}
),

source_wi_svc_line_pct AS (
    SELECT
        serviceordernumber,
        quote_number,
        psg,
        product_id,
        product_key,
        instance_num,
        serial_number,
        quote_net_pid,
        cum_sum,
        cum_pct
    FROM {{ source('raw', 'wi_svc_line_pct') }}
),

final AS (
    SELECT
        quote_number,
        serviceordernumber
    FROM source_wi_svc_line_pct
)

SELECT * FROM final
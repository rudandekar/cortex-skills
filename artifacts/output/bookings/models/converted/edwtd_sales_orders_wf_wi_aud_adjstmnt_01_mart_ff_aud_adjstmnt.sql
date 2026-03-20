{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_aud_adjstmnt', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_AUD_ADJSTMNT',
        'target_table': 'FF_AUD_ADJSTMNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.956002+00:00'
    }
) }}

WITH 

source_wi_aud_adjstmnt AS (
    SELECT
        bk_bookings_trx_type_cd,
        dv_bookings_trx_ref_id,
        sales_order_line_key,
        original_sales_terr_key,
        restatement_type_cd,
        original_cust_prty_key,
        sum_splt,
        bk_fiscal_quarter_number_int,
        bk_fiscal_year_number_int,
        service_indicator_flag,
        product_key
    FROM {{ source('raw', 'wi_aud_adjstmnt') }}
),

final AS (
    SELECT
        bk_bookings_trx_type_cd,
        dv_bookings_trx_ref_id,
        sales_order_line_key,
        original_sales_terr_key,
        restatement_type_cd,
        original_cust_prty_key,
        sum_splt,
        bk_fiscal_quarter_number_int,
        bk_fiscal_year_number_int,
        service_indicator_flag,
        product_key
    FROM source_wi_aud_adjstmnt
)

SELECT * FROM final
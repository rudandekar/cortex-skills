{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_revcloud_quote_lines', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_REVCLOUD_QUOTE_LINES',
        'target_table': 'ST_REVCLOUD_QUOTE_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.958825+00:00'
    }
) }}

WITH 

source_ex_revcloud_quote_lines AS (
    SELECT
        additional_item_info,
        billing_model,
        charge_type,
        created_by,
        created_on,
        duration,
        ext_list_price,
        final_discount,
        final_net_price,
        quote_id,
        line_type,
        non_standard_disc,
        object_id,
        offer_type,
        part_number,
        prepay_term,
        pricing_term,
        quantity,
        subscription_end_date,
        subscription_start_date,
        unit_list_price,
        unit_net_price,
        unit_of_measurement,
        updated_by,
        updated_on,
        parent_line_object_id,
        edw_create_dtm,
        line_number,
        exception_type,
        ai_intent_type_cd,
        trx_ai_product_class_name,
        ai_strategic_factor_pct,
        ai_product_strat_class_nm
    FROM {{ source('raw', 'ex_revcloud_quote_lines') }}
),

source_st_revcloud_quote_lines AS (
    SELECT
        additional_item_info,
        billing_model,
        charge_type,
        created_by,
        created_on,
        duration,
        ext_list_price,
        final_discount,
        final_net_price,
        quote_id,
        line_type,
        non_standard_disc,
        object_id,
        offer_type,
        part_number,
        prepay_term,
        pricing_term,
        quantity,
        subscription_end_date,
        subscription_start_date,
        unit_list_price,
        unit_net_price,
        unit_of_measurement,
        updated_by,
        updated_on,
        parent_line_object_id,
        edw_create_dtm,
        line_number
    FROM {{ source('raw', 'st_revcloud_quote_lines') }}
),

source_el_revcloud_quote_lines AS (
    SELECT
        duration,
        ext_list_price,
        final_discount,
        final_net_price,
        quote_id,
        line_type,
        non_standard_disc,
        object_id,
        offer_type,
        part_number,
        prepay_term,
        pricing_term,
        quantity,
        subscription_end_date,
        subscription_start_date,
        unit_list_price,
        unit_net_price,
        unit_of_measurement,
        parent_line_object_id,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'el_revcloud_quote_lines') }}
),

final AS (
    SELECT
        additional_item_info,
        billing_model,
        charge_type,
        created_by,
        created_on,
        duration,
        ext_list_price,
        final_discount,
        final_net_price,
        quote_id,
        line_type,
        non_standard_disc,
        object_id,
        offer_type,
        part_number,
        prepay_term,
        pricing_term,
        quantity,
        subscription_end_date,
        subscription_start_date,
        unit_list_price,
        unit_net_price,
        unit_of_measurement,
        updated_by,
        updated_on,
        parent_line_object_id,
        edw_create_dtm,
        line_number
    FROM source_el_revcloud_quote_lines
)

SELECT * FROM final
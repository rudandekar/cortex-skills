{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rma_request_line', 'batch', 'edwtd_cs'],
    meta={
        'source_workflow': 'wf_m_WI_RMA_REQUEST_LINE',
        'target_table': 'WI_RMA_REQUEST_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.725445+00:00'
    }
) }}

WITH 

source_st_cmrs_rma_lines AS (
    SELECT
        batch_id,
        ship_to_address_line_3,
        freight_carrier_code,
        m_row$$,
        request_number,
        line_id,
        line_number,
        item,
        user_selectable,
        ordered_quantity,
        selling_price,
        existing_rma_quantity,
        returnable_quantity,
        return_quantity,
        exp_and,
        item_type,
        parent_line_id,
        dependent_child,
        shipment_schedule_line_id,
        creation_date,
        line_creation_date,
        discount_percentage,
        inventory_item_id,
        item_type_code,
        list_price,
        ship_set_number,
        last_updated_by,
        unit_of_measure_code,
        warehouse_id,
        payment_terms,
        payment_terms_id,
        ship_to_customer_id,
        ship_to_customer,
        ship_to_address_id,
        ship_to_address_line_1,
        ship_to_address_line_2,
        ship_to_address_line_4,
        ship_to_address_city,
        ship_to_address_state,
        ship_to_address_zip,
        ship_to_address_county,
        ship_to_address_country,
        ship_to_contact_id,
        ship_to_contact_first_name,
        ship_to_contact_last_name,
        ship_to_contact_email,
        ship_to_contact_status_code,
        revenue_source,
        country,
        return_reference_id,
        ges_update_date,
        global_name,
        create_datetime,
        action_cd
    FROM {{ source('raw', 'st_cmrs_rma_lines') }}
),

final AS (
    SELECT
        bk_awaiting_authorization_num,
        dv_rma_request_submit_date,
        original_sales_order_key,
        rma_request_line_return_qty,
        line_number,
        line_id,
        creation_date
    FROM source_st_cmrs_rma_lines
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_mtl_sales_orders', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_MTL_SALES_ORDERS',
        'target_table': 'PST_CLOUD_MTL_SALES_ORDERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.929609+00:00'
    }
) }}

WITH 

source_cbm_inv_trx_details AS (
    SELECT
        actual_cost,
        cost_group_id,
        cost_type_id,
        creation_date,
        currency_code,
        currency_conversion_rate,
        distribution_account_id,
        encumbrance_account,
        expense_account_id,
        inventory_item_id,
        locator_id,
        material_account,
        material_overhead_account,
        organization_id,
        orig_transaction_quantity,
        overhead_account,
        owning_organization_id,
        pjc_task_id,
        primary_quantity,
        rcv_transaction_id,
        reason_id,
        resource_account,
        revision,
        secondary_transaction_quantity,
        secondary_uom_code,
        source_line_id,
        subinventory_code,
        to_project_id,
        to_task_id,
        transaction_action_id,
        transaction_cost,
        transaction_date,
        transaction_mode,
        transaction_quantity,
        transaction_reference,
        transaction_source_id,
        transaction_source_name,
        transaction_source_type_id,
        transaction_type_id,
        transaction_uom,
        transfer_cost,
        transfer_cost_dist_account,
        transfer_organization_id,
        transfer_transaction_id,
        transfer_subinventory,
        variance_amount,
        last_update_date,
        sales_order_id,
        sales_order_number,
        sales_order_system_id,
        sold_to_party_id,
        source_order_number,
        source_order_type_id,
        transaction_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'cbm_inv_trx_details') }}
),

final AS (
    SELECT
        sales_order_id,
        sales_order_number,
        refresh_datetime,
        offset_number,
        partition_number,
        record_count
    FROM source_cbm_inv_trx_details
)

SELECT * FROM final
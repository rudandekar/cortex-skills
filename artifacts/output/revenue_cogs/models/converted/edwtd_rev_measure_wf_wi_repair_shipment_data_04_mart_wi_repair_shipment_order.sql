{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_repair_shipment_data', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_REPAIR_SHIPMENT_DATA',
        'target_table': 'WI_REPAIR_SHIPMENT_ORDER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.891692+00:00'
    }
) }}

WITH 

source_wi_repair_shipment_order AS (
    SELECT
        repair_order_number,
        repair_order_id,
        source_organization_code,
        item_name,
        ship_date,
        transaction_date,
        repair_quantity,
        transaction_quantity,
        repair_cost,
        std_cost,
        conversion,
        inventory_item_id,
        theater
    FROM {{ source('raw', 'wi_repair_shipment_order') }}
),

source_wi_repair_price_list AS (
    SELECT
        price_list_name,
        loc,
        item_name,
        loc_item,
        price,
        currency_code,
        start_tv_date,
        end_tv_date
    FROM {{ source('raw', 'wi_repair_price_list') }}
),

source_wi_repair_org_price_list AS (
    SELECT
        organization_code,
        repair_price_list
    FROM {{ source('raw', 'wi_repair_org_price_list') }}
),

source_wi_cst_item_costs AS (
    SELECT
        inventory_item_id,
        item_cost
    FROM {{ source('raw', 'wi_cst_item_costs') }}
),

source_wi_ssc_repair_order AS (
    SELECT
        record_type,
        theater,
        item,
        ship_from,
        ship_to,
        global_planner,
        theater_planner,
        document_ref,
        txn_date,
        splc,
        sub_inv,
        quantity,
        uom,
        cost,
        value_1,
        fiscal_month_id,
        created_date,
        load_cycle
    FROM {{ source('raw', 'wi_ssc_repair_order') }}
),

final AS (
    SELECT
        repair_order_number,
        repair_order_id,
        source_organization_code,
        item_name,
        ship_date,
        transaction_date,
        repair_quantity,
        transaction_quantity,
        repair_cost,
        std_cost,
        conversion,
        inventory_item_id,
        theater
    FROM source_wi_ssc_repair_order
)

SELECT * FROM final
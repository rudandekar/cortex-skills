{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_rma_details_b_ship_rcvd', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_EL_RMA_DETAILS_B_SHIP_RCVD',
        'target_table': 'EL_RMA_DETAILS_B_SHIP_RCVD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.803134+00:00'
    }
) }}

WITH 

source_el_rma_details_b_ship_rcvd AS (
    SELECT
        incident_number,
        shipped_pid,
        order_number,
        order_creation_date,
        order_creation_month,
        order_creation_quarter,
        claimed_sn,
        entitlement_result,
        primary_reason,
        actual_shipment_date,
        shipped_sn,
        actual_received_date,
        rma_return_fiscal_month,
        rma_return_fiscal_quarter,
        received_sn,
        received_pid,
        received_sn_on_contract,
        received_sn_on_warranty,
        dv_fiscal_quarter_id
    FROM {{ source('raw', 'el_rma_details_b_ship_rcvd') }}
),

final AS (
    SELECT
        incident_number,
        shipped_pid,
        order_number,
        order_creation_date,
        order_creation_month,
        order_creation_quarter,
        claimed_sn,
        entitlement_result,
        primary_reason,
        actual_shipment_date,
        shipped_sn,
        actual_received_date,
        rma_return_fiscal_month,
        rma_return_fiscal_quarter,
        received_sn,
        received_pid,
        received_sn_on_contract,
        received_sn_on_warranty,
        dv_fiscal_quarter_id
    FROM source_el_rma_details_b_ship_rcvd
)

SELECT * FROM final
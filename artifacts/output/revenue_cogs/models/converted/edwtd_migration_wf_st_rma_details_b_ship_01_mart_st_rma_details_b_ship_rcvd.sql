{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_rma_details_b_ship', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_ST_RMA_DETAILS_B_SHIP',
        'target_table': 'ST_RMA_DETAILS_B_SHIP_RCVD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.586652+00:00'
    }
) }}

WITH 

source_st_rma_details_b_rtrn_only AS (
    SELECT
        order_number,
        received_pid,
        received_sn,
        header_id,
        line_id,
        actual_received_date,
        order_creation_date,
        received_sn_on_contract,
        received_sn_on_warranty,
        edw_create_dtm,
        incident_number,
        shipped_pid,
        claimed_sn,
        entitlement_result,
        primary_reason,
        actual_shipment_date,
        shipped_sn,
        entitlement_audit_id,
        out_rank,
        shipment_line_id,
        received_sn_in_ib,
        received_sn_invalid
    FROM {{ source('raw', 'st_rma_details_b_rtrn_only') }}
),

source_st_rma_details_b_ship_rcvd AS (
    SELECT
        order_number,
        received_pid,
        received_sn,
        header_id,
        line_id,
        actual_received_date,
        order_creation_date,
        received_sn_on_contract,
        received_sn_on_warranty,
        edw_create_dtm,
        incident_number,
        shipped_pid,
        claimed_sn,
        entitlement_result,
        primary_reason,
        actual_shipment_date,
        shipped_sn,
        entitlement_audit_id,
        out_rank,
        shipment_line_id,
        received_sn_in_ib,
        received_sn_invalid
    FROM {{ source('raw', 'st_rma_details_b_ship_rcvd') }}
),

source_st_rma_details_b_received AS (
    SELECT
        order_number,
        received_pid,
        received_sn,
        header_id,
        line_id,
        actual_received_date,
        order_creation_date,
        received_sn_on_contract,
        received_sn_on_warranty,
        edw_create_dtm,
        incident_number,
        shipped_pid,
        claimed_sn,
        entitlement_result,
        primary_reason,
        actual_shipment_date,
        shipped_sn,
        entitlement_audit_id,
        out_rank,
        shipment_line_id,
        received_sn_in_ib,
        received_sn_invalid
    FROM {{ source('raw', 'st_rma_details_b_received') }}
),

final AS (
    SELECT
        order_number,
        received_pid,
        received_sn,
        header_id,
        line_id,
        actual_received_date,
        order_creation_date,
        received_sn_on_contract,
        received_sn_on_warranty,
        edw_create_dtm,
        incident_number,
        shipped_pid,
        claimed_sn,
        entitlement_result,
        primary_reason,
        actual_shipment_date,
        shipped_sn,
        entitlement_audit_id,
        out_rank,
        shipment_line_id,
        received_sn_in_ib,
        received_sn_invalid
    FROM source_st_rma_details_b_received
)

SELECT * FROM final
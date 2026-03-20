{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rcvng_trx_subinv_location', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_RCVNG_TRX_SUBINV_LOCATION',
        'target_table': 'N_RCVNG_TRX_SUBINV_LOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.917102+00:00'
    }
) }}

WITH 

source_w_rcvng_trx_subinv_location AS (
    SELECT
        receiving_transaction_key,
        rcvd_subinventory_location_qty,
        receipt_dtm,
        destination_subinventory_name,
        destination_inventory_org_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rcvng_trx_subinv_location') }}
),

final AS (
    SELECT
        receiving_transaction_key,
        rcvd_subinventory_location_qty,
        receipt_dtm,
        destination_subinventory_name,
        destination_inventory_org_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_rcvng_trx_subinv_location
)

SELECT * FROM final
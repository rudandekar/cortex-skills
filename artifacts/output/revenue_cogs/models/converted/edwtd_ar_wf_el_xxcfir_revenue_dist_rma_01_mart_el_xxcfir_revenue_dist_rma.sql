{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfir_revenue_dist_rma', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFIR_REVENUE_DIST_RMA',
        'target_table': 'EL_XXCFIR_REVENUE_DIST_RMA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.870325+00:00'
    }
) }}

WITH 

source_el_xxcfir_revenue_dist_rma AS (
    SELECT
        transaction_dist_id,
        application_name,
        application_id,
        ledger_id,
        org_id,
        event_id,
        event_type,
        event_status,
        ae_header_id,
        ae_line_num,
        transaction_id,
        trx_number,
        trx_line_id,
        amount,
        account_class,
        header_id,
        order_number,
        order_line_id,
        quantity_invoiced,
        dist_type,
        element_type,
        dist_bal_type,
        source_table_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        attribute1,
        attribute2
    FROM {{ source('raw', 'el_xxcfir_revenue_dist_rma') }}
),

final AS (
    SELECT
        transaction_dist_id,
        application_name,
        application_id,
        ledger_id,
        org_id,
        event_id,
        event_type,
        event_status,
        ae_header_id,
        ae_line_num,
        transaction_id,
        trx_number,
        trx_line_id,
        amount,
        account_class,
        header_id,
        order_number,
        order_line_id,
        quantity_invoiced,
        dist_type,
        element_type,
        dist_bal_type,
        source_table_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        attribute1,
        attribute2
    FROM source_el_xxcfir_revenue_dist_rma
)

SELECT * FROM final
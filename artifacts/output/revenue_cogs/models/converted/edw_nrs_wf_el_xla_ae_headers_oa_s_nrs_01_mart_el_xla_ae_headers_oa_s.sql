{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xla_ae_headers_oa_s_nrs', 'batch', 'edw_nrs'],
    meta={
        'source_workflow': 'wf_m_EL_XLA_AE_HEADERS_OA_S_NRS',
        'target_table': 'EL_XLA_AE_HEADERS_OA_S',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.606914+00:00'
    }
) }}

WITH 

source_el_xla_ae_headers_oa_s AS (
    SELECT
        ae_header_id,
        application_id,
        accounting_date,
        accounting_entry_status_code,
        accounting_entry_type_code,
        balance_type_code,
        completed_date,
        entity_id,
        event_id,
        event_type_code,
        gl_transfer_status_code,
        gl_transfer_date,
        je_category_name,
        ledger_id,
        parent_ae_header_id,
        parent_ae_line_num,
        global_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM {{ source('raw', 'el_xla_ae_headers_oa_s') }}
),

final AS (
    SELECT
        ae_header_id,
        application_id,
        accounting_date,
        accounting_entry_status_code,
        accounting_entry_type_code,
        balance_type_code,
        completed_date,
        entity_id,
        event_id,
        event_type_code,
        gl_transfer_status_code,
        gl_transfer_date,
        je_category_name,
        ledger_id,
        parent_ae_header_id,
        parent_ae_line_num,
        global_name,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM source_el_xla_ae_headers_oa_s
)

SELECT * FROM final
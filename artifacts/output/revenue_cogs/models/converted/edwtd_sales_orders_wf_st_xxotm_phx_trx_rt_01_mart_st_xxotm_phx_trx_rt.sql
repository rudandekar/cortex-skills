{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxotm_phx_trx_rt', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_XXOTM_PHX_TRX_RT',
        'target_table': 'ST_XXOTM_PHX_TRX_RT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.413612+00:00'
    }
) }}

WITH 

source_st_xxotm_phx_trx_rt AS (
    SELECT
        trx_id,
        source_trx_id,
        source_trx_version_id,
        trx_source_type,
        trx_version_id,
        trx_return_line_id,
        trx_return_header_id,
        trx_applied_line_id,
        trx_applied_header_id,
        trx_header_id,
        trx_upstream_code,
        trx_orig_code,
        trx_usage_code,
        trx_type_code,
        deal_id,
        order_quantity,
        cancel_quantity,
        price,
        currency_code,
        change_type_code,
        source_creation_date,
        trx_number,
        trx_value,
        trx_usd_value,
        reporting_date,
        otm_date,
        otm_batch_id,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_xxotm_phx_trx_rt') }}
),

final AS (
    SELECT
        trx_id,
        source_trx_id,
        source_trx_version_id,
        trx_source_type,
        trx_version_id,
        trx_return_line_id,
        trx_return_header_id,
        trx_applied_line_id,
        trx_applied_header_id,
        trx_header_id,
        trx_upstream_code,
        trx_orig_code,
        trx_usage_code,
        trx_type_code,
        deal_id,
        order_quantity,
        cancel_quantity,
        price,
        currency_code,
        change_type_code,
        source_creation_date,
        trx_number,
        trx_value,
        trx_usd_value,
        reporting_date,
        otm_date,
        otm_batch_id,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_st_xxotm_phx_trx_rt
)

SELECT * FROM final
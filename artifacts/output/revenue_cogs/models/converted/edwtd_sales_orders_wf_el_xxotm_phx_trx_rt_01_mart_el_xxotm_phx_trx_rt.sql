{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxotm_phx_trx_rt', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_XXOTM_PHX_TRX_RT',
        'target_table': 'EL_XXOTM_PHX_TRX_RT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.899958+00:00'
    }
) }}

WITH 

source_el_xxotm_phx_trx_rt AS (
    SELECT
        trx_id,
        source_trx_id,
        trx_source_type,
        trx_usage_code,
        trx_type_code,
        create_datetime,
        update_datetime,
        edw_create_user
    FROM {{ source('raw', 'el_xxotm_phx_trx_rt') }}
),

final AS (
    SELECT
        trx_id,
        source_trx_id,
        trx_source_type,
        trx_usage_code,
        trx_type_code,
        create_datetime,
        update_datetime,
        edw_create_user
    FROM source_el_xxotm_phx_trx_rt
)

SELECT * FROM final
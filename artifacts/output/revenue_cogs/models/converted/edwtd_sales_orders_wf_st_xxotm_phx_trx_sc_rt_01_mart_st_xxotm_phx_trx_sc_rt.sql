{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxotm_phx_trx_sc_rt', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_XXOTM_PHX_TRX_SC_RT',
        'target_table': 'ST_XXOTM_PHX_TRX_SC_RT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.504495+00:00'
    }
) }}

WITH 

source_st_xxotm_phx_trx_sc_rt AS (
    SELECT
        trx_sc_id,
        trx_id,
        sc_id,
        sc_sequence,
        trx_split_id,
        total_split_percent,
        sc_split_percent,
        trx_split_percent,
        latest_sc_flag,
        parent_child_flag,
        otm_batch_id,
        object_version_number,
        sc_duplicate_flag,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        start_date,
        expiration_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_xxotm_phx_trx_sc_rt') }}
),

final AS (
    SELECT
        trx_sc_id,
        trx_id,
        sc_id,
        sc_sequence,
        trx_split_id,
        total_split_percent,
        sc_split_percent,
        trx_split_percent,
        latest_sc_flag,
        parent_child_flag,
        otm_batch_id,
        object_version_number,
        sc_duplicate_flag,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        start_date,
        expiration_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_st_xxotm_phx_trx_sc_rt
)

SELECT * FROM final
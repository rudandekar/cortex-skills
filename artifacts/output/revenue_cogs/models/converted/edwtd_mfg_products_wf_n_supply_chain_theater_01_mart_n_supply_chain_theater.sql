{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_supply_chain_theater', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SUPPLY_CHAIN_THEATER',
        'target_table': 'N_SUPPLY_CHAIN_THEATER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.814683+00:00'
    }
) }}

WITH 

source_w_supply_chain_theater AS (
    SELECT
        bk_supply_chain_theater_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_supply_chain_theater') }}
),

final AS (
    SELECT
        bk_supply_chain_theater_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_supply_chain_theater
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_supply_chain_theater_country', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SUPPLY_CHAIN_THEATER_COUNTRY',
        'target_table': 'N_SUPPLY_CHAIN_THEATER_COUNTRY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.624514+00:00'
    }
) }}

WITH 

source_w_supply_chain_theater_country AS (
    SELECT
        bk_iso_country_code,
        bk_supply_chain_theater_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_supply_chain_theater_country') }}
),

final AS (
    SELECT
        bk_iso_country_code,
        bk_supply_chain_theater_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_supply_chain_theater_country
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_supply_chain_theater_country', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_SUPPLY_CHAIN_THEATER_COUNTRY',
        'target_table': 'W_SUPPLY_CHAIN_THEATER_COUNTRY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.561634+00:00'
    }
) }}

WITH 

source_st_cg1_wsh_regions_tl AS (
    SELECT
        language_r,
        source_lang,
        region_id,
        continent,
        country,
        country_region,
        state,
        city,
        zone_r,
        postal_code_from,
        postal_code_to,
        alternate_name,
        county,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_wsh_regions_tl') }}
),

final AS (
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
    FROM source_st_cg1_wsh_regions_tl
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_tss_sites_maps_v', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_TSS_SITES_MAPS_V',
        'target_table': 'W_TSS_SITES_MAPS_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.068652+00:00'
    }
) }}

WITH 

source_st_tac_tss_sites_map_v AS (
    SELECT
        bl_site_key,
        cust_theater,
        cust_country,
        creation_date,
        bl_last_update_date,
        party_id,
        party_site_id,
        cust_account_id,
        cust_acct_site_id
    FROM {{ source('raw', 'st_tac_tss_sites_map_v') }}
),

final AS (
    SELECT
        bl_site_key,
        cust_theater,
        cust_country,
        creation_date,
        bl_last_update_date,
        party_id,
        party_site_id,
        theater,
        country,
        party_name,
        cust_account_id,
        cust_acct_site_id
    FROM source_st_tac_tss_sites_map_v
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_mf_hz_parties_ap', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_MF_HZ_PARTIES_AP',
        'target_table': 'EL_MF_HZ_PARTIES_AP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.625715+00:00'
    }
) }}

WITH 

source_st_mf_hz_parties_ap AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        ges_update_date,
        global_name,
        language_name,
        last_updated_by,
        last_update_date,
        orig_system_reference,
        party_id,
        party_name,
        party_number,
        party_type,
        postal_code,
        known_as,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_hz_parties_ap') }}
),

transformed_exp_map_cols AS (
    SELECT
    batch_id,
    created_by,
    creation_date,
    ges_update_date,
    global_name,
    language_name,
    last_updated_by,
    last_update_date,
    orig_system_reference,
    party_id,
    party_name,
    party_number,
    party_type,
    postal_code,
    known_as,
    create_datetime,
    action_code
    FROM source_st_mf_hz_parties_ap
),

final AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        ges_update_date,
        global_name,
        language_name,
        last_updated_by,
        last_update_date,
        orig_system_reference,
        party_id,
        party_name,
        party_number,
        party_type,
        postal_code,
        known_as,
        create_datetime,
        action_code
    FROM transformed_exp_map_cols
)

SELECT * FROM final
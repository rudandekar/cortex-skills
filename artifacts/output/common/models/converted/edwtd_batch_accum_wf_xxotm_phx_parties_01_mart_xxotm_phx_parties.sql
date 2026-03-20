{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_xxotm_phx_parties', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_XXOTM_PHX_PARTIES',
        'target_table': 'XXOTM_PHX_PARTIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.794827+00:00'
    }
) }}

WITH 

source_stg_xxotm_phx_parties AS (
    SELECT
        otm_customer_id,
        name,
        country,
        city,
        state,
        cr_party_id,
        cr_party_id_source,
        region,
        county,
        postal_code,
        address1,
        address2,
        address3,
        otm_batch_id,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_xxotm_phx_parties') }}
),

source_g2c_xxotm_phx_parties AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        otm_customer_id,
        name,
        country,
        city,
        state,
        cr_party_id,
        cr_party_id_source,
        region,
        county,
        postal_code,
        address1,
        address2,
        address3,
        otm_batch_id,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login
    FROM {{ source('raw', 'g2c_xxotm_phx_parties') }}
),

transformed_exp_xxotm_phx_parties AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    otm_customer_id,
    name,
    country,
    city,
    state,
    cr_party_id,
    cr_party_id_source,
    region,
    county,
    postal_code,
    address1,
    address2,
    address3,
    otm_batch_id,
    object_version_number,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login
    FROM source_g2c_xxotm_phx_parties
),

final AS (
    SELECT
        otm_customer_id,
        name,
        country,
        city,
        state,
        cr_party_id,
        cr_party_id_source,
        region,
        county,
        postal_code,
        address1,
        address2,
        address3,
        otm_batch_id,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_xxotm_phx_parties
)

SELECT * FROM final
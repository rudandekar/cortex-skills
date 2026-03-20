{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_eac_group_mem_sav_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_EAC_GROUP_MEM_SAV_V',
        'target_table': 'ST_XXFSAM_EAC_GROUP_MEM_SAV_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.376474+00:00'
    }
) }}

WITH 

source_ff_xxfsam_eac_group_mem_sav_v AS (
    SELECT
        eac_group_id,
        eac_group_dtl_id,
        sav_id,
        effective_date,
        expiration_date,
        last_update_date,
        last_updated_by,
        organization_name,
        organization_id,
        status_name,
        creation_date,
        created_by
    FROM {{ source('raw', 'ff_xxfsam_eac_group_mem_sav_v') }}
),

final AS (
    SELECT
        eac_group_dtl_id,
        eac_group_id,
        sav_id,
        effective_date,
        expiration_date,
        last_update_date,
        last_updated_by,
        organization_name,
        organization_id,
        status_name,
        creation_date,
        created_by
    FROM source_ff_xxfsam_eac_group_mem_sav_v
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_eac_group_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_EAC_GROUP_V',
        'target_table': 'ST_XXFSAM_EAC_GROUP_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.807099+00:00'
    }
) }}

WITH 

source_ff_xxfsam_eac_group_v AS (
    SELECT
        eac_group_id,
        eac_group_name,
        eac_start_date,
        eac_end_date,
        eac_creation_date,
        eac_created_by,
        eac_last_update_date,
        eac_last_updated_by,
        eac_group_vertical_name,
        eac_group_sub_vertical_name,
        eac_group_market_segment,
        eac_group_subsegment,
        organization_name,
        organization_id,
        eac_group_status,
        batch_id
    FROM {{ source('raw', 'ff_xxfsam_eac_group_v') }}
),

final AS (
    SELECT
        eac_group_id,
        eac_group_name,
        eac_start_date,
        eac_end_date,
        eac_creation_date,
        eac_created_by,
        eac_last_update_date,
        eac_last_updated_by,
        eac_group_vertical_name,
        eac_group_sub_vertical_name,
        eac_group_market_segment,
        eac_group_subsegment,
        organization_name,
        organization_id,
        eac_group_status,
        batch_id
    FROM source_ff_xxfsam_eac_group_v
)

SELECT * FROM final
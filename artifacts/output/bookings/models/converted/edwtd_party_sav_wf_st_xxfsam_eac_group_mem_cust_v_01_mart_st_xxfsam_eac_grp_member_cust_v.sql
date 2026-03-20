{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxfsam_eac_group_member_cust_v', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_ST_XXFSAM_EAC_GROUP_MEMBER_CUST_V',
        'target_table': 'ST_XXFSAM_EAC_GRP_MEMBER_CUST_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.838317+00:00'
    }
) }}

WITH 

source_ff_xxfsam_eac_group_mem_cust_v AS (
    SELECT
        eac_group_id,
        customer_party_bu,
        effective_date,
        expiration_date,
        last_update_date
    FROM {{ source('raw', 'ff_xxfsam_eac_group_mem_cust_v') }}
),

final AS (
    SELECT
        eac_group_id,
        customer_party_bu,
        effective_date,
        expiration_date,
        last_update_date
    FROM source_ff_xxfsam_eac_group_mem_cust_v
)

SELECT * FROM final
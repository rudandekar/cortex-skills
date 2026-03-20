{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcts_dm_fnd_usr_map_wtcalc', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_XXCTS_DM_FND_USR_MAP_WTCALC',
        'target_table': 'ST_XXCTS_DM_FND_USR_MAP_WTCALC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.181635+00:00'
    }
) }}

WITH 

source_ff_xxcts_dm_fnd_usr_map_wtcalc AS (
    SELECT
        r11_user_id,
        r12_user_id
    FROM {{ source('raw', 'ff_xxcts_dm_fnd_usr_map_wtcalc') }}
),

transformed_exp_datatransform AS (
    SELECT
    r11_user_id,
    r12_user_id,
    IFF(LTRIM(RTRIM(R11_USER_ID)) = '\N',-999, TO_INTEGER(R11_USER_ID)) AS o_r11_user_id,
    IFF(LTRIM(RTRIM(R12_USER_ID)) = '\N',-999,TO_INTEGER(R12_USER_ID)) AS o_r12_user_id
    FROM source_ff_xxcts_dm_fnd_usr_map_wtcalc
),

final AS (
    SELECT
        r11_user_id,
        r12_user_id
    FROM transformed_exp_datatransform
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_tac_centers_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_TAC_CENTERS_D',
        'target_table': 'EL_TSS_TAC_CENTERS_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.585830+00:00'
    }
) }}

WITH 

source_st_tss_tac_centers_d AS (
    SELECT
        bl_tac_center_key,
        tac_center_id,
        tac_center_name,
        nlp_reporting_flag,
        enabled_flag,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_last_update_date
    FROM {{ source('raw', 'st_tss_tac_centers_d') }}
),

final AS (
    SELECT
        bl_tac_center_key,
        tac_center_id,
        tac_center_name,
        nlp_reporting_flag,
        enabled_flag,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_last_update_date,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_tss_tac_centers_d
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_cot_tech_d_wtcalc', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_COT_TECH_D_WTCALC',
        'target_table': 'EL_TSS_COT_TECH_D_WTCALC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.625777+00:00'
    }
) }}

WITH 

source_st_tss_cot_tech_d_wtcalc AS (
    SELECT
        bl_cot_technology_key,
        tech_id,
        tech_name,
        sub_tech_id,
        sub_tech_name,
        tech_active_flag,
        sub_tech_active_flag,
        bl_creation_date,
        bl_last_update_date
    FROM {{ source('raw', 'st_tss_cot_tech_d_wtcalc') }}
),

transformed_tgt_expr AS (
    SELECT
    bl_cot_technology_key,
    tech_id,
    tech_name,
    sub_tech_id,
    sub_tech_name,
    tech_active_flag,
    sub_tech_active_flag,
    bl_creation_date,
    bl_last_update_date
    FROM source_st_tss_cot_tech_d_wtcalc
),

final AS (
    SELECT
        bl_cot_technology_key,
        tech_id,
        tech_name,
        sub_tech_id,
        sub_tech_name,
        tech_active_flag,
        sub_tech_active_flag,
        bl_creation_date,
        bl_last_update_date,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM transformed_tgt_expr
)

SELECT * FROM final
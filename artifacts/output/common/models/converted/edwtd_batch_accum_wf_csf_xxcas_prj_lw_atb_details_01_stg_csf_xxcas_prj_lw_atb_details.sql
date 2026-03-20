{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_prj_lw_atb_details', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_LW_ATB_DETAILS',
        'target_table': 'STG_CSF_XXCAS_PRJ_LW_ATB_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.559254+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_lw_atb_details AS (
    SELECT
        arch_id_seq,
        sku_id,
        business_service_name,
        architecture_name,
        technology_name,
        sub_technology_name,
        creation_date,
        last_update_date,
        last_updated_by,
        created_by,
        last_update_login,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_lw_atb_details') }}
),

source_csf_xxcas_prj_lw_atb_details AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        arch_id_seq,
        sku_id,
        business_service_name,
        architecture_name,
        technology_name,
        sub_technology_name,
        creation_date,
        last_update_date,
        last_updated_by,
        created_by,
        last_update_login,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        business_id,
        architecture_id,
        tech_id,
        sub_tech_id
    FROM {{ source('raw', 'csf_xxcas_prj_lw_atb_details') }}
),

transformed_exp_csf_xxcas_prj_lw_atb_details AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    arch_id_seq,
    sku_id,
    business_service_name,
    architecture_name,
    technology_name,
    sub_technology_name,
    creation_date,
    last_update_date,
    last_updated_by,
    created_by,
    last_update_login,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    business_id,
    architecture_id,
    tech_id,
    sub_tech_id
    FROM source_csf_xxcas_prj_lw_atb_details
),

final AS (
    SELECT
        arch_id_seq,
        sku_id,
        business_service_name,
        architecture_name,
        technology_name,
        sub_technology_name,
        creation_date,
        last_update_date,
        last_updated_by,
        created_by,
        last_update_login,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_lw_atb_details
)

SELECT * FROM final
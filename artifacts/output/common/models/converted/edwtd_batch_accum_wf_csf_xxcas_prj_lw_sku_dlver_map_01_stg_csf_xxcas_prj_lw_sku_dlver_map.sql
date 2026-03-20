{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_prj_lw_sku_dlver_map', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_LW_SKU_DLVER_MAP',
        'target_table': 'STG_CSF_XXCAS_PRJ_LW_SKU_DLVER_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.762257+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_lw_sku_dlver_map AS (
    SELECT
        dlvrble_id,
        sku_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        dlvrble_number,
        dlvrble_name,
        dlvrble_type,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_lw_sku_dlver_map') }}
),

source_csf_xxcas_prj_lw_sku_dlr_mp AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        dlvrble_id,
        sku_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        dlvrble_number,
        dlvrble_name,
        dlvrble_type
    FROM {{ source('raw', 'csf_xxcas_prj_lw_sku_dlr_mp') }}
),

transformed_exp_csf_xxcas_prj_lw_sku_dlver_map AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    dlvrble_id,
    sku_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    dlvrble_number,
    dlvrble_name,
    dlvrble_type
    FROM source_csf_xxcas_prj_lw_sku_dlr_mp
),

final AS (
    SELECT
        dlvrble_id,
        sku_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        dlvrble_number,
        dlvrble_name,
        dlvrble_type,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_lw_sku_dlver_map
)

SELECT * FROM final
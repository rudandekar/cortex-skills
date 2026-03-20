{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_prj_cst_frst_prt_dst', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_CST_FRST_PRT_DST',
        'target_table': 'STG_CSF_XXCAS_PRJ_CST_FRST_PRT_DST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.707524+00:00'
    }
) }}

WITH 

source_csf_xxcas_prj_cst_frst_prt_dst AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        partner_po_id,
        period_name,
        percentage,
        notes,
        forecast_amount,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        ogg_key_id
    FROM {{ source('raw', 'csf_xxcas_prj_cst_frst_prt_dst') }}
),

source_stg_csf_xxcas_prj_cst_frst_prt_dst AS (
    SELECT
        partner_po_id,
        period_name,
        percentage,
        notes,
        forecast_amount,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_cst_frst_prt_dst') }}
),

transformed_exp_csf_xxcas_prj_cst_frst_prt_dst AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    partner_po_id,
    period_name,
    percentage,
    notes,
    forecast_amount,
    last_update_login,
    created_by,
    creation_date,
    last_update_date,
    last_updated_by,
    ogg_key_id
    FROM source_stg_csf_xxcas_prj_cst_frst_prt_dst
),

final AS (
    SELECT
        partner_po_id,
        period_name,
        percentage,
        notes,
        forecast_amount,
        last_update_login,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_cst_frst_prt_dst
)

SELECT * FROM final
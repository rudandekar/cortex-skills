{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_oe_hold_releases_hldnrt', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_OE_HOLD_RELEASES_HLDNRT',
        'target_table': 'ST_CG1_OE_HOLD_RELEASES_HLDNRT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.224741+00:00'
    }
) }}

WITH 

source_cg1_oe_hold_releases AS (
    SELECT
        hold_release_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        program_application_id,
        program_id,
        program_update_date,
        request_id,
        hold_source_id,
        release_reason_code,
        release_comment,
        context,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        order_hold_id,
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'cg1_oe_hold_releases') }}
),

final AS (
    SELECT
        hold_release_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        program_application_id,
        program_id,
        program_update_date,
        request_id,
        hold_source_id,
        release_reason_code,
        release_comment,
        context,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        order_hold_id,
        source_commit_time,
        global_name,
        create_datetime,
        source_dml_type,
        refresh_datetime
    FROM source_cg1_oe_hold_releases
)

SELECT * FROM final
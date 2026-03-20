{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxscm_pl_early_build_dac', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXSCM_PL_EARLY_BUILD_DAC',
        'target_table': 'ST_CG1_XXSCM_PL_EARLY_BUILD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.902850+00:00'
    }
) }}

WITH 

source_cg1_xxscm_pl_early_build_data AS (
    SELECT
        build_id,
        order_number,
        ship_set_number,
        start_date,
        status,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        error_msg,
        request_id,
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
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_xxscm_pl_early_build_data') }}
),

final AS (
    SELECT
        build_id,
        order_number,
        ship_set_number,
        start_date,
        status,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        error_msg,
        request_id,
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
        global_name,
        batch_id,
        create_datetime,
        action_cd,
        source_commit_time,
        refresh_datetime
    FROM source_cg1_xxscm_pl_early_build_data
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxscm_dv_4c1_ib_iface', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXSCM_DV_4C1_IB_IFACE',
        'target_table': 'ST_CG1_XXSCM_DV_4C1_IB_IFACE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.443149+00:00'
    }
) }}

WITH 

source_cg1_xxscm_dv_4c1_ib_iface AS (
    SELECT
        message_id,
        cycle_count_id,
        response_date,
        handling_unit_id,
        handling_unit_id_status,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
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
        slc_site_name,
        part_duns_number,
        cisco_duns_number,
        b2b_process_status,
        seq_id,
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'cg1_xxscm_dv_4c1_ib_iface') }}
),

final AS (
    SELECT
        message_id,
        cycle_count_id,
        response_date,
        handling_unit_id,
        handling_unit_id_status,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
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
        slc_site_name,
        part_duns_number,
        cisco_duns_number,
        b2b_process_status,
        seq_id,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_cg1_xxscm_dv_4c1_ib_iface
)

SELECT * FROM final
{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_xxscm_mk_carton_ib_iface', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXSCM_MK_CARTON_IB_IFACE',
        'target_table': 'STG_CG1_XXSCM_MK_CRTN_IB_IFACE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.702207+00:00'
    }
) }}

WITH 

source_cg1_xxscm_mk_carton_ib_iface AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        message_bom_pack_id,
        packout_date,
        carton_id,
        carton_dim_id,
        bp_serial_number,
        weight,
        height,
        length,
        width,
        carton_dim_uom,
        carton_weight_uom,
        parent_carton_id,
        sj_process_status,
        process_status,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        request_id,
        attribute_context,
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
        lpn_process_status,
        lpn_process_date,
        carton_status
    FROM {{ source('raw', 'cg1_xxscm_mk_carton_ib_iface') }}
),

source_stg_cg1_xxscm_mk_crtn_ib_iface AS (
    SELECT
        message_bom_pack_id,
        packout_date,
        carton_id,
        bp_serial_number,
        weight,
        carton_dim_uom,
        carton_weight_uom,
        parent_carton_id,
        process_status,
        last_update_date,
        attribute14,
        attribute15,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxscm_mk_crtn_ib_iface') }}
),

transformed_exp_cg1_xxscm_mk_carton_ib_iface AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    message_bom_pack_id,
    packout_date,
    carton_id,
    bp_serial_number,
    weight,
    carton_dim_uom,
    carton_weight_uom,
    parent_carton_id,
    process_status,
    last_update_date,
    attribute14,
    attribute15
    FROM source_stg_cg1_xxscm_mk_crtn_ib_iface
),

final AS (
    SELECT
        message_bom_pack_id,
        packout_date,
        carton_id,
        bp_serial_number,
        weight,
        carton_dim_uom,
        carton_weight_uom,
        parent_carton_id,
        process_status,
        last_update_date,
        attribute14,
        attribute15,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxscm_mk_carton_ib_iface
)

SELECT * FROM final
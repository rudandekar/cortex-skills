{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxscm_mk_carton', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_XXSCM_MK_CARTON',
        'target_table': 'EL_XXSCM_MK_CARTON',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.176074+00:00'
    }
) }}

WITH 

source_st_cg1_xxscm_mk_carton_ib_i AS (
    SELECT
        carton_id,
        message_bom_pack_id,
        packout_date,
        carton_dim_id,
        bp_serial_number,
        weight,
        height,
        length1,
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
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_xxscm_mk_carton_ib_i') }}
),

final AS (
    SELECT
        carton_id,
        message_bom_pack_id,
        packout_date,
        carton_dim_id,
        bp_serial_number,
        weight,
        height,
        length1,
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
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM source_st_cg1_xxscm_mk_carton_ib_i
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxscm_mk_asnline_ib', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXSCM_MK_ASNLINE_IB',
        'target_table': 'ST_CG1_XXSCM_MK_ASNLINE_IB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.195749+00:00'
    }
) }}

WITH 

source_cg1_xxscm_mk_asnline_ib AS (
    SELECT
        asn_line_id,
        ship_set_number,
        inventory_item_number,
        shipped_quantity,
        uom,
        serial_number,
        asn_processed_flag,
        asn_processed_status,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        serial_num_status,
        message_3b2_id,
        container_id,
        po_number,
        po_release_line_number,
        source_dml_type,
        source_commit_time,
        trail_file_name,
        refresh_datetime
    FROM {{ source('raw', 'cg1_xxscm_mk_asnline_ib') }}
),

final AS (
    SELECT
        asn_line_id,
        ship_set_number,
        inventory_item_number,
        shipped_quantity,
        uom,
        serial_number,
        asn_processed_flag,
        asn_processed_status,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        serial_num_status,
        message_3b2_id,
        container_id,
        po_number,
        po_release_line_number,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_cg1_xxscm_mk_asnline_ib
)

SELECT * FROM final
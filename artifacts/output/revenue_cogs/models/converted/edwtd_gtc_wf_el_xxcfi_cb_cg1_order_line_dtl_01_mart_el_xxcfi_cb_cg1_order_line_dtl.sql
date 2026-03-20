{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfi_cb_cg1_order_line_dtls', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFI_CB_CG1_ORDER_LINE_DTLS',
        'target_table': 'EL_XXCFI_CB_CG1_ORDER_LINE_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.718975+00:00'
    }
) }}

WITH 

source_st_xxcfi_cb_cg1_ordr_lne_dtls AS (
    SELECT
        line_seq_id,
        order_number,
        route_code,
        shipset_number,
        ship_to,
        ship_from,
        header_id,
        line_id,
        ship_date,
        inventory_item_id,
        item_name,
        occ_country_code,
        dslc_country_code,
        status,
        ior,
        orig_sys_document_ref,
        orig_sys_line_ref,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        org_id,
        schedule_ship_date,
        schedule_arrival_date,
        item_description,
        leg_0_flag,
        leg_0_hts_check,
        leg_0_name,
        leg_0_export_country,
        leg_0_import_country,
        leg_1_flag,
        leg_1_hts_check,
        leg_1_name,
        leg_1_export_country,
        leg_1_import_country,
        leg_2_flag,
        leg_2_hts_check,
        leg_2_name,
        leg_2_export_country,
        leg_2_import_country,
        pick_release_point,
        leg_0_pick_release,
        leg_1_pick_release,
        leg_2_pick_release,
        ior_type
    FROM {{ source('raw', 'st_xxcfi_cb_cg1_ordr_lne_dtls') }}
),

final AS (
    SELECT
        line_seq_id,
        order_number,
        shipset_number,
        org_id,
        src,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_st_xxcfi_cb_cg1_ordr_lne_dtls
)

SELECT * FROM final
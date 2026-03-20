{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_warranty_xxccs_ds_instance_detail', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_EL_WARRANTY_XXCCS_DS_INSTANCE_DETAIL',
        'target_table': 'EL_WARRANTY_XXCCS_DS_INSTANCE_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.987869+00:00'
    }
) }}

WITH 

source_el_warranty_xxccs_ds_instance_detail AS (
    SELECT
        instance_id,
        inventory_item_id,
        serial_number,
        dup_serial_number,
        quantity,
        unit_of_measure,
        instance_status_desc,
        active_start_date,
        active_end_date,
        warranty_type,
        warranty_coverage_template,
        warranty_end_date,
        instance_creation_date,
        nrt_last_update_date,
        so_number,
        po_number,
        attribute1,
        install_location_id,
        install_at_site_use_id,
        ship_to_site_use_id,
        ship_to_party_id,
        bill_to_site_use_id,
        bill_to_party_id,
        parent_instance_id
    FROM {{ source('raw', 'el_warranty_xxccs_ds_instance_detail') }}
),

final AS (
    SELECT
        instance_id,
        inventory_item_id,
        serial_number,
        dup_serial_number,
        quantity,
        unit_of_measure,
        instance_status_desc,
        active_start_date,
        active_end_date,
        warranty_type,
        warranty_coverage_template,
        warranty_end_date,
        instance_creation_date,
        nrt_last_update_date,
        so_number,
        po_number,
        attribute1,
        install_location_id,
        install_at_site_use_id,
        ship_to_site_use_id,
        ship_to_party_id,
        bill_to_site_use_id,
        bill_to_party_id,
        parent_instance_id
    FROM source_el_warranty_xxccs_ds_instance_detail
)

SELECT * FROM final
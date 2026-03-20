{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cbm_inv_id_mapping_pre', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_EL_CBM_INV_ID_MAPPING_PRE',
        'target_table': 'EL_CBM_INV_ID_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.717100+00:00'
    }
) }}

WITH 

source_pst_cbm_inv_id_mapping AS (
    SELECT
        organization_id,
        organization_code,
        kit_item_id,
        kit_item,
        kit_item_decription,
        kite_item_revision,
        componet_item_id,
        componet_item,
        component_item_decription,
        component_item_revision,
        component_effectivity_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'pst_cbm_inv_id_mapping') }}
),

final AS (
    SELECT
        organization_id,
        organization_code,
        component_inv_item_id,
        kit_item,
        kit_item_decription,
        kite_item_revision,
        cloud_inv_item_id,
        componet_item,
        component_item_decription,
        component_item_revision,
        component_effectivity_date,
        component_disable_date,
        offset_number,
        partition_number,
        record_count
    FROM source_pst_cbm_inv_id_mapping
)

SELECT * FROM final
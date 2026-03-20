{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_cst_standard_costs', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_CST_STANDARD_COSTS',
        'target_table': 'PST_CLOUD_CST_STANDARD_COSTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.941394+00:00'
    }
) }}

WITH 

source_cbm_cst_std_cost_dtls AS (
    SELECT
        effective_end_date,
        creation_date,
        last_update_date,
        std_cost_id,
        last_updated_by,
        previous_effective_end_date,
        cost_org_id,
        cost_book_id,
        total_cost,
        val_structure_id,
        effective_start_date,
        cost_profile_id,
        cost_element_id,
        unit_cost,
        previous_effective_start_date,
        status_code,
        inventory_item_id,
        std_cost_detail_id,
        uom_code,
        val_unit_id,
        previous_total_cost,
        cost_level_code,
        inventory_org_id,
        previous_std_cost_id,
        currency_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'cbm_cst_std_cost_dtls') }}
),

final AS (
    SELECT
        effective_end_date,
        creation_date,
        last_update_date,
        std_cost_id,
        last_updated_by,
        previous_effective_end_date,
        cost_org_id,
        cost_book_id,
        total_cost,
        val_structure_id,
        effective_start_date,
        cost_profile_id,
        cost_element_id,
        unit_cost,
        previous_effective_start_date,
        status_code,
        inventory_item_id,
        std_cost_detail_id,
        uom_code,
        val_unit_id,
        previous_total_cost,
        cost_level_code,
        inventory_org_id,
        previous_std_cost_id,
        currency_code,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM source_cbm_cst_std_cost_dtls
)

SELECT * FROM final
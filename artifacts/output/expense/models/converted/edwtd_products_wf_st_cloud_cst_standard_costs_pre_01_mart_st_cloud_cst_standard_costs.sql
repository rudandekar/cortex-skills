{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cloud_cst_standard_costs_pre', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_ST_CLOUD_CST_STANDARD_COSTS_PRE',
        'target_table': 'ST_CLOUD_CST_STANDARD_COSTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.854886+00:00'
    }
) }}

WITH 

source_pst_cloud_cst_standard_costs AS (
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
    FROM {{ source('raw', 'pst_cloud_cst_standard_costs') }}
),

final AS (
    SELECT
        cost_update_id,
        inventory_item_id,
        organization_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        standard_cost_revision_date,
        standard_cost,
        std_cost_detail_id,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        global_name,
        offset_number,
        partition_number,
        record_count
    FROM source_pst_cloud_cst_standard_costs
)

SELECT * FROM final
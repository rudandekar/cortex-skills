{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_hr_locations_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_HR_LOCATIONS_ALL',
        'target_table': 'ST_MF_HR_LOCATIONS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.706786+00:00'
    }
) }}

WITH 

source_ff_mf_hr_locations_all AS (
    SELECT
        batch_id,
        address_line_1,
        address_line_2,
        address_line_3,
        bill_to_site_flag,
        country,
        derived_locale,
        description,
        designated_receiver_id,
        in_organization_flag,
        inactive_date,
        inventory_organization_id,
        location_code,
        location_id,
        object_version_number,
        office_site_flag,
        postal_code,
        receiving_site_flag,
        region_1,
        region_2,
        ship_to_location_id,
        ship_to_site_flag,
        style,
        tax_name,
        telephone_number_1,
        telephone_number_2,
        telephone_number_3,
        town_or_city,
        tp_header_id,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_hr_locations_all') }}
),

final AS (
    SELECT
        batch_id,
        address_line_1,
        address_line_2,
        address_line_3,
        bill_to_site_flag,
        country,
        derived_locale,
        description,
        designated_receiver_id,
        in_organization_flag,
        inactive_date,
        inventory_organization_id,
        location_code,
        location_id,
        object_version_number,
        office_site_flag,
        postal_code,
        receiving_site_flag,
        region_1,
        region_2,
        ship_to_location_id,
        ship_to_site_flag,
        style,
        tax_name,
        telephone_number_1,
        telephone_number_2,
        telephone_number_3,
        town_or_city,
        tp_header_id,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM source_ff_mf_hr_locations_all
)

SELECT * FROM final
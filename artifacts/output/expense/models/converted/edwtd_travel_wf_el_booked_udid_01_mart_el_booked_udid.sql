{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_booked_udid', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_EL_BOOKED_UDID',
        'target_table': 'EL_BOOKED_UDID',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.965307+00:00'
    }
) }}

WITH 

source_st_booked_udid AS (
    SELECT
        batch_id,
        xpk_row6,
        fk_travel6,
        pnr_locator6,
        udid_number,
        udid_info,
        pax_sequence6,
        udid_indicator,
        gds_code6,
        customer_id6,
        create_datetime,
        action_code,
        replaced_udid_info
    FROM {{ source('raw', 'st_booked_udid') }}
),

final AS (
    SELECT
        xpk_row6,
        fk_travel6,
        pnr_locator6,
        udid_number,
        udid_info,
        pax_sequence6,
        udid_indicator,
        gds_code6,
        customer_id6,
        deleted_flag,
        create_datetime,
        update_datetime,
        replaced_udid_info
    FROM source_st_booked_udid
)

SELECT * FROM final
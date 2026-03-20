{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_dfr_gross_unbil_rebate_data', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_DFR_GROSS_UNBIL_REBATE_DATA',
        'target_table': 'EL_DFR_GROSS_UNBIL_REBATE_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.530297+00:00'
    }
) }}

WITH 

source_el_dfr_gross_unbil_rebate_data AS (
    SELECT
        business_unit,
        classification,
        rebate_pct,
        active_from,
        active_to,
        active_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user
    FROM {{ source('raw', 'el_dfr_gross_unbil_rebate_data') }}
),

final AS (
    SELECT
        business_unit,
        classification,
        rebate_pct,
        active_from,
        active_to,
        active_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user
    FROM source_el_dfr_gross_unbil_rebate_data
)

SELECT * FROM final
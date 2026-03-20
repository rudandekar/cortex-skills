{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_om_cfi_adj_lookups', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_OM_CFI_ADJ_LOOKUPS',
        'target_table': 'EL_OM_CFI_ADJ_LOOKUPS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.468035+00:00'
    }
) }}

WITH 

source_st_om_cfi_adj_lookups AS (
    SELECT
        batch_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        description,
        enabled_flag,
        end_date,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        lookup_id,
        lookup_type,
        lookup_value,
        start_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cfi_adj_lookups') }}
),

final AS (
    SELECT
        lookup_id,
        description,
        global_name,
        lookup_type,
        lookup_value
    FROM source_st_om_cfi_adj_lookups
)

SELECT * FROM final
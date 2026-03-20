{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_direct_corp_adj_type_lvl2', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_DIRECT_CORP_ADJ_TYPE_LVL2',
        'target_table': 'EX_OM_CFI_ADJ_LOOKUPS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.431132+00:00'
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
        action_code,
        exception_type
    FROM source_st_om_cfi_adj_lookups
)

SELECT * FROM final
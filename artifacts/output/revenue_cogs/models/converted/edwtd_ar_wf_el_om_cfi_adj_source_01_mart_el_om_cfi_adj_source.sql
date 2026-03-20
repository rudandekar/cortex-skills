{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_om_cfi_adj_source', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_OM_CFI_ADJ_SOURCE',
        'target_table': 'EL_OM_CFI_ADJ_SOURCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.253858+00:00'
    }
) }}

WITH 

source_st_om_cfi_adj_source AS (
    SELECT
        batch_id,
        adj_source_id,
        adj_source_type,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cfi_adj_source') }}
),

final AS (
    SELECT
        adj_source_id,
        adj_source_type,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        global_name
    FROM source_st_om_cfi_adj_source
)

SELECT * FROM final
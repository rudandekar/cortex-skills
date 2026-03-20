{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_element_type', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_ELEMENT_TYPE',
        'target_table': 'W_CCRM_ELEMENT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.803658+00:00'
    }
) }}

WITH 

source_st_ngccrm_lookup_details AS (
    SELECT
        batch_id,
        details_id,
        lookup_id,
        detail_code,
        detail_meaning,
        detail_desc,
        enabled_flag,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ngccrm_lookup_details') }}
),

final AS (
    SELECT
        bk_ccrm_element_type_cd,
        element_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ngccrm_lookup_details
)

SELECT * FROM final
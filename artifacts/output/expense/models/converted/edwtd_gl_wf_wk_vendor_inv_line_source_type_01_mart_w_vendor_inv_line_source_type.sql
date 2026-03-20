{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_vendor_inv_line_source_type', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_VENDOR_INV_LINE_SOURCE_TYPE',
        'target_table': 'W_VENDOR_INV_LINE_SOURCE_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.704749+00:00'
    }
) }}

WITH 

source_el_fnd_lookup_values AS (
    SELECT
        lookup_type,
        lang,
        security_group_id,
        lookup_code,
        meaning,
        view_application_id,
        global_name,
        enabled_flag,
        description,
        create_datetime,
        ges_update_date,
        start_date_active,
        end_date_active,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5
    FROM {{ source('raw', 'el_fnd_lookup_values') }}
),

final AS (
    SELECT
        bk_vendor_inv_line_src_type_cd,
        vendor_inv_line_src_type_name,
        vendor_inv_line_src_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_el_fnd_lookup_values
)

SELECT * FROM final
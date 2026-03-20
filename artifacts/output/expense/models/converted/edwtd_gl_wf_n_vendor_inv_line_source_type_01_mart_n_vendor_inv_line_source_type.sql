{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_vendor_inv_line_source_type ', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_VENDOR_INV_LINE_SOURCE_TYPE ',
        'target_table': 'N_VENDOR_INV_LINE_SOURCE_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.006210+00:00'
    }
) }}

WITH 

source_w_vendor_inv_line_source_type AS (
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
    FROM {{ source('raw', 'w_vendor_inv_line_source_type') }}
),

final AS (
    SELECT
        bk_vendor_inv_line_src_type_cd,
        vendor_inv_line_src_type_name,
        vendor_inv_line_src_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_vendor_inv_line_source_type
)

SELECT * FROM final
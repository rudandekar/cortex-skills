{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rr_srvc_category_wk', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_RR_SRVC_CATEGORY_WK',
        'target_table': 'WI_GL_RR_SRVC_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.602078+00:00'
    }
) }}

WITH 

source_wi_gl_rr_srvc_category AS (
    SELECT
        bk_service_category_id,
        bk_allocated_servc_group_id,
        product_subgroup_id,
        allocation_percentage,
        item_key
    FROM {{ source('raw', 'wi_gl_rr_srvc_category') }}
),

final AS (
    SELECT
        bk_service_category_id,
        bk_allocated_servc_group_id,
        product_subgroup_id,
        allocation_percentage,
        item_key
    FROM source_wi_gl_rr_srvc_category
)

SELECT * FROM final
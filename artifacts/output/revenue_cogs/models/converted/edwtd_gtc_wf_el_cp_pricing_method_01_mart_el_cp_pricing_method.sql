{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cp_pricing_method', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CP_PRICING_METHOD',
        'target_table': 'EL_CP_PRICING_METHOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.232570+00:00'
    }
) }}

WITH 

source_el_cp_pricing_method AS (
    SELECT
        price_method_id,
        method_name,
        ss_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'el_cp_pricing_method') }}
),

final AS (
    SELECT
        price_method_id,
        method_name,
        ss_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_el_cp_pricing_method
)

SELECT * FROM final
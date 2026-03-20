{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tbl_product_id_mapping', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_TBL_PRODUCT_ID_MAPPING',
        'target_table': 'ST_TBL_PRODUCT_ID_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.136114+00:00'
    }
) }}

WITH 

source_ff_tbl_product_id_mapping AS (
    SELECT
        product_id,
        iot_flag,
        cisco_one_flag,
        fast_track_flag,
        sbtg_flag
    FROM {{ source('raw', 'ff_tbl_product_id_mapping') }}
),

final AS (
    SELECT
        product_id,
        iot_flag,
        cisco_one_flag,
        fast_track_flag,
        sbtg_flag
    FROM source_ff_tbl_product_id_mapping
)

SELECT * FROM final
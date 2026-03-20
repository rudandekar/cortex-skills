{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_w_product_fast_track', 'batch', 'edwtd_rdc'],
    meta={
        'source_workflow': 'wf_m_W_PRODUCT_FAST_TRACK',
        'target_table': 'W_PRODUCT_FAST_TRACK_TEMP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.926342+00:00'
    }
) }}

WITH 

source_st_product_fast_track AS (
    SELECT
        product_id,
        ft3_flag
    FROM {{ source('raw', 'st_product_fast_track') }}
),

final AS (
    SELECT
        product_key,
        fast_track_world_wide_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_product_fast_track
)

SELECT * FROM final
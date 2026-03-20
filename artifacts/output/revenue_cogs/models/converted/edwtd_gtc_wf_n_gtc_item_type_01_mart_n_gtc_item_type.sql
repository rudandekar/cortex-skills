{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_gtc_item_type', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_GTC_ITEM_TYPE',
        'target_table': 'N_GTC_ITEM_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.932938+00:00'
    }
) }}

WITH 

source_w_gtc_item_type AS (
    SELECT
        bk_gtc_item_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_gtc_item_type') }}
),

final AS (
    SELECT
        bk_gtc_item_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_gtc_item_type
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_gtc_product_type_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_GTC_PRODUCT_TYPE_STG23NF',
        'target_table': 'N_GTC_PRODUCT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.847744+00:00'
    }
) }}

WITH 

source_n_gtc_product_type AS (
    SELECT
        gtc_product_type_cd,
        ss_cd,
        product_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_gtc_product_type') }}
),

final AS (
    SELECT
        gtc_product_type_cd,
        ss_cd,
        product_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_gtc_product_type
)

SELECT * FROM final
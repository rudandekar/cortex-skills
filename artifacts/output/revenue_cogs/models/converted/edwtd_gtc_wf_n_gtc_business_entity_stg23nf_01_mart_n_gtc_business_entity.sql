{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_gtc_business_entity_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_GTC_BUSINESS_ENTITY_STG23NF',
        'target_table': 'N_GTC_BUSINESS_ENTITY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.566778+00:00'
    }
) }}

WITH 

source_n_gtc_business_entity AS (
    SELECT
        bk_gtc_business_entity_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_gtc_business_entity') }}
),

final AS (
    SELECT
        bk_gtc_business_entity_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_gtc_business_entity
)

SELECT * FROM final
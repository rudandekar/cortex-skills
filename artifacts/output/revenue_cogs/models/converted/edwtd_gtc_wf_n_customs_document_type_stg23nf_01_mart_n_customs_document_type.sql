{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_customs_document_type_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_CUSTOMS_DOCUMENT_TYPE_STG23NF',
        'target_table': 'N_CUSTOMS_DOCUMENT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.440195+00:00'
    }
) }}

WITH 

source_n_customs_document_type AS (
    SELECT
        bk_customs_document_type_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_customs_document_type') }}
),

final AS (
    SELECT
        bk_customs_document_type_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_customs_document_type
)

SELECT * FROM final
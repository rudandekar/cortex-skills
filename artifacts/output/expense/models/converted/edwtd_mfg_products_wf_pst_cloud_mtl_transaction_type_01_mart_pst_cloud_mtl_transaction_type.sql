{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_clould_mtl_transaction_type', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_PST_CLOULD_MTL_TRANSACTION_TYPE',
        'target_table': 'PST_CLOUD_MTL_TRANSACTION_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.621921+00:00'
    }
) }}

WITH 

source_cbm_mtl_transaction_types AS (
    SELECT
        created_by,
        creation_date,
        description,
        language,
        last_update_date,
        last_updated_by,
        source_lang,
        transaction_type_id,
        transaction_type_name,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'cbm_mtl_transaction_types') }}
),

final AS (
    SELECT
        created_by,
        creation_date,
        description,
        language1,
        last_update_date,
        last_updated_by,
        source_lang,
        transaction_type_id,
        transaction_type_name,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM source_cbm_mtl_transaction_types
)

SELECT * FROM final
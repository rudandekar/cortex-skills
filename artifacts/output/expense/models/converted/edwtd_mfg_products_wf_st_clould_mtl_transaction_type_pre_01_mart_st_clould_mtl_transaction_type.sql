{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_clould_mtl_transaction_type_pre', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CLOULD_MTL_TRANSACTION_TYPE_PRE',
        'target_table': 'ST_CLOULD_MTL_TRANSACTION_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.621219+00:00'
    }
) }}

WITH 

source_pst_cloud_mtl_transaction_type AS (
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
    FROM {{ source('raw', 'pst_cloud_mtl_transaction_type') }}
),

final AS (
    SELECT
        transaction_type_name,
        transaction_source_type_id,
        disable_date,
        description,
        transaction_type_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        offset_number,
        partition_number,
        record_count
    FROM source_pst_cloud_mtl_transaction_type
)

SELECT * FROM final
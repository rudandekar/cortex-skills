{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cloud_mtl_transaction_reas_pre', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CLOUD_MTL_TRANSACTION_REAS_PRE',
        'target_table': 'ST_CLOUD_MTL_TRANSACTION_REAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.887711+00:00'
    }
) }}

WITH 

source_pst_cloud_mtl_transaction_reas AS (
    SELECT
        creation_date,
        description,
        last_update_date,
        last_updated_by,
        reason_id,
        reason_name,
        reason_type,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        record_count
    FROM {{ source('raw', 'pst_cloud_mtl_transaction_reas') }}
),

final AS (
    SELECT
        reason_id,
        reason_name,
        last_update_login,
        reason_type,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        reason_type_display,
        reason_context_code,
        ges_update_date,
        global_name,
        create_datetime,
        offset_number,
        partition_number,
        record_count
    FROM source_pst_cloud_mtl_transaction_reas
)

SELECT * FROM final
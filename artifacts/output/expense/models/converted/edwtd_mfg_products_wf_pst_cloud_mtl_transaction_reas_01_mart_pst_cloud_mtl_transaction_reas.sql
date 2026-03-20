{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_pst_cloud_mtl_transaction_reas', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_PST_CLOUD_MTL_TRANSACTION_REAS',
        'target_table': 'PST_CLOUD_MTL_TRANSACTION_REAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.618291+00:00'
    }
) }}

WITH 

source_cbm_inv_transaction_reasons AS (
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
    FROM {{ source('raw', 'cbm_inv_transaction_reasons') }}
),

transformed_exp_pst_cloud_mtl_transaction_reas AS (
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
    record_count,
    SUBSTR(CREATION_DATE,0,19) AS creation_date_new,
    SUBSTR(LAST_UPDATE_DATE,0,19) AS last_update_date_new
    FROM source_cbm_inv_transaction_reasons
),

final AS (
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
    FROM transformed_exp_pst_cloud_mtl_transaction_reas
)

SELECT * FROM final
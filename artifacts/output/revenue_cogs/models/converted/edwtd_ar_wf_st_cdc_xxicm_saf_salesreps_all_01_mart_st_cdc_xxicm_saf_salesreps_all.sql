{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cdc_xxicm_saf_salesreps_all', 'realtime', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CDC_XXICM_SAF_SALESREPS_ALL',
        'target_table': 'ST_CDC_XXICM_SAF_SALESREPS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.821881+00:00'
    }
) }}

WITH 

source_cdc_xxicm_saf_salesreps_all AS (
    SELECT
        saf_salesrep_id,
        saf_invoice_id,
        salesrep_id,
        territory_id,
        split_percent,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        transaction_type,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cdc_xxicm_saf_salesreps_all') }}
),

final AS (
    SELECT
        batch_id,
        saf_salesrep_id,
        saf_invoice_id,
        salesrep_id,
        territory_id,
        split_percent,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        transaction_type,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM source_cdc_xxicm_saf_salesreps_all
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cg_xxicm_saf_salesreps_all', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_CG_XXICM_SAF_SALESREPS_ALL',
        'target_table': 'EL_CG_XXICM_SAF_SALESREPS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.843463+00:00'
    }
) }}

WITH 

source_st_cg_xxicm_saf_salesreps_all AS (
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
    FROM {{ source('raw', 'st_cg_xxicm_saf_salesreps_all') }}
),

final AS (
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
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM source_st_cg_xxicm_saf_salesreps_all
)

SELECT * FROM final
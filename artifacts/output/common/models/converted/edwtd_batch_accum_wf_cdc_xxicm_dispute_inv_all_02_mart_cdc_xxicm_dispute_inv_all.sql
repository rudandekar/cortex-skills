{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cdc_xxicm_dispute_inv_all', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CDC_XXICM_DISPUTE_INV_ALL',
        'target_table': 'CDC_XXICM_DISPUTE_INV_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.012640+00:00'
    }
) }}

WITH 

source_stg_cdc_xxicm_dispute_inv_all AS (
    SELECT
        dispute_saf_trx_id,
        saf_id,
        dispute_id,
        customer_trx_id,
        class_code,
        included_in_saf,
        dispute_section,
        amount,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cdc_xxicm_dispute_inv_all') }}
),

source_cdc_xxicm_dispute_inv_all AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        dispute_saf_trx_id,
        saf_id,
        dispute_id,
        customer_trx_id,
        class,
        included_in_saf,
        dispute_section,
        amount,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login
    FROM {{ source('raw', 'cdc_xxicm_dispute_inv_all') }}
),

transformed_exp_cdc_xxicm_dispute_inv_all AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    dispute_saf_trx_id,
    saf_id,
    dispute_id,
    customer_trx_id,
    class,
    included_in_saf,
    dispute_section,
    amount,
    org_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login
    FROM source_cdc_xxicm_dispute_inv_all
),

final AS (
    SELECT
        dispute_saf_trx_id,
        saf_id,
        dispute_id,
        customer_trx_id,
        class_code,
        included_in_saf,
        dispute_section,
        amount,
        org_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cdc_xxicm_dispute_inv_all
)

SELECT * FROM final
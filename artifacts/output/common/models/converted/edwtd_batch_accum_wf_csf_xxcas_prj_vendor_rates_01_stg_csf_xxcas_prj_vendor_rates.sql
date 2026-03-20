{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_prj_vendor_rates', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_VENDOR_RATES',
        'target_table': 'STG_CSF_XXCAS_PRJ_VENDOR_RATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.748308+00:00'
    }
) }}

WITH 

source_csf_xxcas_prj_vendor_rates AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        vendor_employee_number,
        vendor_company_name,
        weekly_rate,
        sow_currency,
        start_date_active,
        end_date_active,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        enabled_flag,
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
        seq_num,
        ogg_key_id
    FROM {{ source('raw', 'csf_xxcas_prj_vendor_rates') }}
),

source_stg_csf_xxcas_prj_vendor_rates AS (
    SELECT
        vendor_employee_number,
        vendor_company_name,
        weekly_rate,
        sow_currency,
        start_date_active,
        end_date_active,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        enabled_flag,
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
        seq_num,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_vendor_rates') }}
),

transformed_exp_csf_xxcas_prj_vendor_rates AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    vendor_employee_number,
    vendor_company_name,
    weekly_rate,
    sow_currency,
    start_date_active,
    end_date_active,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    enabled_flag,
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
    seq_num,
    ogg_key_id
    FROM source_stg_csf_xxcas_prj_vendor_rates
),

final AS (
    SELECT
        vendor_employee_number,
        vendor_company_name,
        weekly_rate,
        sow_currency,
        start_date_active,
        end_date_active,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        enabled_flag,
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
        seq_num,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_vendor_rates
)

SELECT * FROM final
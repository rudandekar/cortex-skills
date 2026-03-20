{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cp_adjustment_rates_bv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CP_ADJUSTMENT_RATES_BV',
        'target_table': 'ST_CP_ADJUSTMENT_RATES_BV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.645723+00:00'
    }
) }}

WITH 

source_bv_cp_adjustment_rates AS (
    SELECT
        adjustment_rate_id,
        rate_set_id,
        owner1,
        partner,
        ar_qualifier1,
        ar_qualifier2,
        effective_from_date,
        effective_to_date,
        rate,
        rate_displayed,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        secondary_rate,
        secondary_rate_displayed,
        source_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'bv_cp_adjustment_rates') }}
),

final AS (
    SELECT
        adjustment_rate_id,
        rate_set_id,
        owner1,
        partner,
        ar_qualifier1,
        ar_qualifier2,
        effective_from_date,
        effective_to_date,
        rate,
        rate_displayed,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        secondary_rate,
        secondary_rate_displayed,
        source_id,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_bv_cp_adjustment_rates
)

SELECT * FROM final
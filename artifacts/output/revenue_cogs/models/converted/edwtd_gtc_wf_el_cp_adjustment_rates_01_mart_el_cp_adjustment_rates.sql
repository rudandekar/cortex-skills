{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cp_adjustment_rates', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CP_ADJUSTMENT_RATES',
        'target_table': 'EL_CP_ADJUSTMENT_RATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.317883+00:00'
    }
) }}

WITH 

source_el_cp_adjustment_rates AS (
    SELECT
        adjustment_rate_id,
        rate_set_id,
        owner,
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
        ss_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'el_cp_adjustment_rates') }}
),

final AS (
    SELECT
        adjustment_rate_id,
        rate_set_id,
        owner,
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
        ss_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_el_cp_adjustment_rates
)

SELECT * FROM final
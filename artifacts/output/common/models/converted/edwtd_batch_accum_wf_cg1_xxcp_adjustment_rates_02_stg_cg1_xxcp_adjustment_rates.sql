{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_xxcp_adjustment_rates', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXCP_ADJUSTMENT_RATES',
        'target_table': 'STG_CG1_XXCP_ADJUSTMENT_RATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.075679+00:00'
    }
) }}

WITH 

source_cg1_xxcp_adjustment_rates AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
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
        secondary_rate,
        secondary_rate_displayed,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        source_id,
        project_tag,
        project_action,
        project_action_ovr,
        last_project_tag
    FROM {{ source('raw', 'cg1_xxcp_adjustment_rates') }}
),

source_stg_cg1_xxcp_adjustment_rates AS (
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
        secondary_rate,
        secondary_rate_displayed,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        source_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxcp_adjustment_rates') }}
),

transformed_exp_cg1_xxcp_adjustment_rates AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
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
    secondary_rate,
    secondary_rate_displayed,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    md_id,
    source_id
    FROM source_stg_cg1_xxcp_adjustment_rates
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
        secondary_rate,
        secondary_rate_displayed,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        md_id,
        source_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxcp_adjustment_rates
)

SELECT * FROM final
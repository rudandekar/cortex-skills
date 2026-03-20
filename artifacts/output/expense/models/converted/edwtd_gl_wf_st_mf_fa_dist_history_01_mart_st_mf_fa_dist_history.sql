{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_fa_dist_history', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_FA_DIST_HISTORY',
        'target_table': 'ST_MF_FA_DIST_HISTORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.133024+00:00'
    }
) }}

WITH 

source_ff_mf_fa_dist_history AS (
    SELECT
        batch_id,
        asset_id,
        assigned_to,
        book_type_code,
        code_combination_id,
        date_effective,
        date_ineffective,
        distribution_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        location_id,
        retirement_id,
        transaction_header_id_in,
        transaction_header_id_out,
        transaction_units,
        units_assigned,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_fa_dist_history') }}
),

final AS (
    SELECT
        batch_id,
        asset_id,
        assigned_to,
        book_type_code,
        code_combination_id,
        date_effective,
        date_ineffective,
        distribution_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        location_id,
        retirement_id,
        transaction_header_id_in,
        transaction_header_id_out,
        transaction_units,
        units_assigned,
        create_datetime,
        action_code
    FROM source_ff_mf_fa_dist_history
)

SELECT * FROM final
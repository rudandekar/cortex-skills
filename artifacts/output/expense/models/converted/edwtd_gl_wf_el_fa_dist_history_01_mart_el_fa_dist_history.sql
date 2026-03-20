{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_fa_dist_history', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_FA_DIST_HISTORY',
        'target_table': 'EL_FA_DIST_HISTORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.822480+00:00'
    }
) }}

WITH 

source_st_mf_fa_dist_history AS (
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
    FROM {{ source('raw', 'st_mf_fa_dist_history') }}
),

final AS (
    SELECT
        asset_id,
        assigned_to,
        book_type_code,
        code_combination_id,
        date_effective,
        date_ineffective,
        distribution_id,
        global_name,
        location_id,
        transaction_header_id_in,
        transaction_header_id_out,
        transaction_units,
        units_assigned
    FROM source_st_mf_fa_dist_history
)

SELECT * FROM final